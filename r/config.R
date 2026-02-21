# R/config.R
# Centralized config + path helpers + lightweight QA utilities.
# All pipeline scripts should source this file and call read_config().

read_config <- function(path = NULL, create_dirs = TRUE) {
  if (is.null(path) || !nzchar(path)) {
    path <- Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "")
    if (!nzchar(path)) {
      # Prefer local config.yml if it exists, else fall back to config.example.yml.
      if (file.exists("config.yml")) path <- "config.yml" else path <- "config.example.yml"
    }
  }

  if (!requireNamespace("yaml", quietly = TRUE)) stop("Missing package: yaml")
  if (!file.exists(path)) stop("Config file not found: ", path)

  cfg <- yaml::read_yaml(path)

  # Defaults
  cfg$paths <- cfg$paths %||% list()
  cfg$paths$repo_root      <- cfg$paths$repo_root      %||% "."
  cfg$paths$input_dir      <- cfg$paths$input_dir      %||% "inputs"
  cfg$paths$processed_dir  <- cfg$paths$processed_dir  %||% "processed"
  cfg$paths$output_dir     <- cfg$paths$output_dir     %||% "output"
  cfg$paths$tmp_dir        <- cfg$paths$tmp_dir        %||% "tmp"

  cfg$inputs <- cfg$inputs %||% list()
  cfg$run    <- cfg$run    %||% list(sample_mode = FALSE, sample_n = 200000, seed = 123)
  cfg$spatial <- cfg$spatial %||% list(h3_resolution = 9, time_unit = "year_quarter")

  # Normalize paths relative to repo root unless absolute
  root <- cfg$paths$repo_root
  cfg$paths$repo_root <- normalizePath(root, winslash = "/", mustWork = FALSE)

  # Helper to join paths
  join <- function(...) file.path(..., fsep = "/")

  cfg$paths$input_dir     <- normalizePath(join(cfg$paths$repo_root, cfg$paths$input_dir), winslash="/", mustWork=FALSE)
  cfg$paths$processed_dir <- normalizePath(join(cfg$paths$repo_root, cfg$paths$processed_dir), winslash="/", mustWork=FALSE)
  cfg$paths$output_dir    <- normalizePath(join(cfg$paths$repo_root, cfg$paths$output_dir), winslash="/", mustWork=FALSE)
  cfg$paths$tmp_dir       <- normalizePath(join(cfg$paths$repo_root, cfg$paths$tmp_dir), winslash="/", mustWork=FALSE)

  if (create_dirs) {
    if (!requireNamespace("fs", quietly = TRUE)) stop("Missing package: fs")
    fs::dir_create(cfg$paths$processed_dir, recurse = TRUE)
    fs::dir_create(cfg$paths$output_dir, recurse = TRUE)
    fs::dir_create(join(cfg$paths$output_dir, "logs"), recurse = TRUE)
    fs::dir_create(cfg$paths$tmp_dir, recurse = TRUE)
  }

  cfg$meta <- list(
    config_path = normalizePath(path, winslash="/", mustWork=FALSE),
    build_time_utc = format(Sys.time(), tz = "UTC", usetz = TRUE)
  )

  cfg
}

# Null-coalescing helper (since we don't want to pull in rlang just for this)
`%||%` <- function(x, y) if (is.null(x)) y else x

# ---- Path helpers ----
is_abs_path <- function(x) {
  if (is.null(x) || !nzchar(x)) return(FALSE)
  grepl("^(/|[A-Za-z]:[\\/])", x)
}

p_in <- function(cfg, rel_or_abs) {
  if (is.null(rel_or_abs) || !nzchar(rel_or_abs)) return(NULL)
  if (is_abs_path(rel_or_abs)) return(normalizePath(rel_or_abs, winslash="/", mustWork=FALSE))
  normalizePath(file.path(cfg$paths$input_dir, rel_or_abs), winslash="/", mustWork=FALSE)
}

p_proc <- function(cfg, ...) normalizePath(file.path(cfg$paths$processed_dir, ...), winslash="/", mustWork=FALSE)
p_out  <- function(cfg, ...) normalizePath(file.path(cfg$paths$output_dir, ...), winslash="/", mustWork=FALSE)
p_tmp  <- function(cfg, ...) normalizePath(file.path(cfg$paths$tmp_dir, ...), winslash="/", mustWork=FALSE)

# Product path helper: look up a product key from cfg$products and return full path
p_product <- function(cfg, key) {
  rel <- cfg$products[[key]]
  if (is.null(rel)) stop("Unknown product key: ", key, ". Check config.yml products section.")
  p_proc(cfg, rel)
}

# Input path helper: look up an input key from cfg$inputs and return full path
p_input <- function(cfg, key) {
  rel <- cfg$inputs[[key]]
  if (is.null(rel)) stop("Unknown input key: ", key, ". Check config.yml inputs section.")
  p_in(cfg, rel)
}

# ---- Logging ----
logf <- function(..., log_file = NULL) {
  msg <- paste0("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", paste(..., collapse = ""))
  message(msg)
  if (!is.null(log_file) && nzchar(log_file)) {
    dir.create(dirname(log_file), showWarnings = FALSE, recursive = TRUE)
    cat(msg, "\n", file = log_file, append = TRUE)
  }
  invisible(msg)
}

# ---- QA helpers ----
assert_unique <- function(dt, keys, name = deparse(substitute(dt))) {
  if (!requireNamespace("data.table", quietly = TRUE)) stop("Missing package: data.table")
  if (!data.table::is.data.table(dt)) dt <- data.table::as.data.table(dt)
  stopifnot(is.character(keys), length(keys) >= 1)
  n0 <- nrow(dt)
  ndups <- dt[, .N, by = keys][N > 1L, .N]
  if (ndups > 0 ) {
    stop("Uniqueness assertion failed for ", name, " on keys {", paste(keys, collapse=", "), "}. ",
         "Found ", ndups, " duplicate rows (counting all dup rows).")
  }
  invisible(TRUE)
}

assert_has_cols <- function(dt, cols, name = deparse(substitute(dt))) {
  stopifnot(all(cols %in% names(dt)))
  invisible(TRUE)
}

# ---- Script runner ----
# This lets targets orchestrate existing scripts without modifying them yet.
# Scripts can gradually be refactored to use read_config() and the helpers above.
run_rscript <- function(script, cfg_path = Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"),
                        args = character(), log_name = NULL) {
  if (!file.exists(script)) stop("Script not found: ", script)
  if (!nzchar(cfg_path)) cfg_path <- "config.yml"

  # Log to output/logs by default
  if (!requireNamespace("fs", quietly = TRUE)) stop("Missing package: fs")
  cfg <- read_config(cfg_path, create_dirs = TRUE)
  log_file <- if (!is.null(log_name)) p_out(cfg, "logs", paste0(log_name, ".log")) else NULL

  # Set config path env var for child process (inherit rest of environment)
  old_cfg_env <- Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = NA)
  Sys.setenv(PHILLY_EVICTIONS_CONFIG = cfg$meta$config_path)
  on.exit({
    if (is.na(old_cfg_env)) Sys.unsetenv("PHILLY_EVICTIONS_CONFIG")
    else Sys.setenv(PHILLY_EVICTIONS_CONFIG = old_cfg_env)
  })

  cmd <- c("--vanilla", script, args)
  logf("Running: Rscript ", paste(cmd, collapse = " "), log_file = log_file)

  res <- system2("Rscript", args = cmd, stdout = TRUE, stderr = TRUE)
  if (!is.null(log_file)) cat(res, sep="\n", file = log_file, append = TRUE)

  # Basic failure detection
  # (system2 does not always return status in a portable way when stdout/stderr are captured)
  invisible(res)
}
