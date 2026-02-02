## ============================================================
## assign-evictions-crimes-homes-to-hexagon.r
## ============================================================
## Purpose: Assign evictions and crimes to H3 hexagonal cells and create
##          a panel dataset aggregated by hex and year-quarter
##
## Inputs:
##   - cfg$inputs$crime (crime/philly-crime.csv)
##   - cfg$products$evictions_clean (clean/evictions_clean.csv)
##   - cfg$inputs$hex_res{7,8,9} (shapefiles/philly_hex_res{7,8,9})
##
## Outputs:
##   - cfg$products$hex_panel_res{7,8,9} (panels/phl_hex_yq_panel_res{7,8,9}.csv)
##
## Primary key: (h3, year_quarter)
##
## Note: Resolution is configurable via cfg$spatial$h3_resolution (default 9)
## ============================================================

# ---- Threads / libs ----
Sys.setenv("OMP_THREAD_LIMIT" = max(1, parallel::detectCores() %/% 2))
data.table::setDTthreads(max(1, parallel::detectCores() %/% 2))

suppressPackageStartupMessages({
  library(data.table)
  library(sf)
  library(h3jsr)
  library(lubridate)
  library(tidyverse)
})

# ---- Load config and set up logging ----
source("r/config.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "assign-evictions-crimes-homes-to-hexagon.log")

logf("=== Starting assign-evictions-crimes-homes-to-hexagon.r ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ---- Configuration ----
RES <- cfg$spatial$h3_resolution %||% 9
logf("H3 resolution: ", RES, log_file = log_file)

# Column names for crime data
lat_col_crime <- "lat"
lon_col_crime <- "lng"
date_col_crime <- "dispatch_date"

# Column names for eviction data
lat_col_ev <- "latitude"
lon_col_ev <- "longitude"
date_col_ev <- "d_filing"

# ---- Helper functions ----
source("r/helper-functions.R")

ensure_wgs84 <- function(g) {
  if (is.na(sf::st_crs(g))) sf::st_set_crs(g, 4326)
  else if (sf::st_crs(g)$epsg != 4326) sf::st_transform(g, 4326) else g
}

to_yq <- function(d) {
  d <- as.IDate(d)
  sprintf("%d_Q%d", year(d), quarter(d))
}

# Ring counts for spatial spillovers
ring_counts_one_quarter <- function(h3_vec, counts_vec, N = 3) {
  stopifnot(length(h3_vec) == length(counts_vec))
  dt <- data.table(h3 = h3_vec, ring_0 = counts_vec)
  lut <- copy(dt)
  setnames(lut, "ring_0", "val")
  setkey(lut, h3)
  for (k in seq_len(N)) {
    rings <- h3jsr::get_ring(h3_vec, ring_size = k, simple = TRUE)
    nb_dt <- data.table(
      h3_center = rep(h3_vec, lengths(rings)),
      h3_nb = unlist(rings, use.names = FALSE)
    )
    nb_dt <- nb_dt[lut, on = .(h3_nb = h3), nomatch = 0L]
    ring_sum <- nb_dt[, .(v = sum(val, na.rm = TRUE)), by = h3_center]
    setnames(ring_sum, c("h3_center", "v"), c("h3", paste0("ring_", k)))
    dt <- ring_sum[dt, on = "h3"]
    set(dt, which(is.na(dt[[paste0("ring_", k)]])), paste0("ring_", k), 0)
  }
  dt[]
}

add_ring_leads_lags <- function(dt, ring_prefix = "crime_ring_", max_lag = 8, max_lead = 4) {
  setDT(dt)
  yq <- dt[, tstrsplit(year_quarter, "_Q", fixed = TRUE)]
  dt[, `:=`(.__Y__ = as.integer(yq[[1]]), .__Q__ = as.integer(yq[[2]]))]
  dt[, .__T__ := (.__Y__ * 4L + .__Q__)]
  ring_cols <- grep(paste0("^", ring_prefix), names(dt), value = TRUE)
  setkey(dt, h3, .__T__)
  for (col in ring_cols) {
    for (L in seq_len(max_lag)) {
      dt[, paste0(col, "_lag_", L) := shift(get(col), n = L, type = "lag"), by = h3]
    }
    for (F in seq_len(max_lead)) {
      dt[, paste0(col, "_lead_", F) := shift(get(col), n = F, type = "lead"), by = h3]
    }
  }
  dt[, c(".__Y__", ".__Q__", ".__T__") := NULL][]
}

# ---- Load hexes ----
logf("Loading hex shapefile...", log_file = log_file)

hex_key <- paste0("hex_res", RES)
hex_path <- p_input(cfg, hex_key)

# The hex path might be a directory containing the shapefile
if (dir.exists(hex_path)) {
  hex_shp <- file.path(hex_path, sprintf("phl_hex_res%d.shp", RES))
} else {
  hex_shp <- hex_path
}

logf("  Hex path: ", hex_shp, log_file = log_file)
hex_sf <- st_read(hex_shp, quiet = TRUE) |> ensure_wgs84()
stopifnot("h3" %in% names(hex_sf))
hex_ids <- hex_sf$h3
logf("  Loaded ", length(hex_ids), " hexagons", log_file = log_file)

# ---- Load & tag CRIMES ----
logf("Loading crime data...", log_file = log_file)

crime_path <- p_input(cfg, "crime")
logf("  Crime path: ", crime_path, log_file = log_file)
crime <- fread(crime_path, showProgress = FALSE)
crime <- crime[!is.na(get(lat_col_crime)) & !is.na(get(lon_col_crime))]
logf("  Loaded ", nrow(crime), " crime records with coordinates", log_file = log_file)

# Crime categories
violent <- c(
  "Homicide - Criminal", "Homicide - Justifiable", "Homicide - Gross Negligence",
  "Rape", "Robbery Firearm", "Robbery No Firearm",
  "Aggravated Assault Firearm", "Aggravated Assault No Firearm"
)

property <- c(
  "Thefts", "Theft from Vehicle", "Motor Vehicle Theft", "Receiving Stolen Property",
  "Vandalism/Criminal Mischief", "Arson", "Fraud",
  "Forgery and Counterfeiting", "Embezzlement"
)

nuisance <- c(
  "Disorderly Conduct", "Public Drunkenness", "DRIVING UNDER THE INFLUENCE",
  "Liquor Law Violations", "Vagrancy/Loitering", "Gambling Violations",
  "Prostitution and Commercialized Vice", "Narcotic / Drug Law Violations",
  "Weapon Violations"
)

crime[, crime_cat := fcase(
  text_general_code %chin% violent, "Violent",
  text_general_code == "Other Assaults", "Simple Assault",
  grepl("^Burglary", text_general_code), "Burglary",
  text_general_code %chin% property, "Property",
  text_general_code %chin% nuisance, "Nuisance",
  default = "Other"
)]

# Parse dates
d_c <- suppressWarnings(ymd(crime[[date_col_crime]]))
bad <- is.na(d_c)
if (any(bad)) d_c[bad] <- suppressWarnings(ymd_hms(crime[[date_col_crime]][bad]))
crime[, date := as.IDate(d_c)]
crime <- crime[!is.na(date)]
crime[, year_quarter := to_yq(date)]

# Spatial join to hexes
crime_sf <- st_as_sf(crime, coords = c(lon_col_crime, lat_col_crime), crs = 4326, remove = FALSE)
crime_in_hex <- st_join(crime_sf, hex_sf[, "h3"], left = FALSE, join = st_within)
crime_dt <- as.data.table(st_drop_geometry(crime_in_hex))

# Count crimes by type
crime_counts_long <- crime_dt[, .(crime_count = .N), by = .(h3, year_quarter, crime_cat)]
crime_counts <- dcast(crime_counts_long, h3 + year_quarter ~ crime_cat, value.var = "crime_count", fill = 0)
setnames(
  crime_counts,
  old = setdiff(names(crime_counts), c("h3", "year_quarter")),
  new = paste0("crime_count_", setdiff(names(crime_counts), c("h3", "year_quarter")))
)

logf("  Crime counts aggregated to ", nrow(crime_counts), " hex-quarter combinations", log_file = log_file)

# ---- Load & tag EVICTIONS ----
logf("Loading eviction data...", log_file = log_file)

evict_path <- p_product(cfg, "evictions_clean")
logf("  Eviction path: ", evict_path, log_file = log_file)
ev <- fread(evict_path, showProgress = FALSE)
ev <- ev[!is.na(get(lat_col_ev)) & !is.na(get(lon_col_ev))]
logf("  Loaded ", nrow(ev), " eviction records with coordinates", log_file = log_file)

# Parse dates
d_e <- suppressWarnings(ymd(ev[[date_col_ev]]))
bad <- is.na(d_e)
if (any(bad)) d_e[bad] <- suppressWarnings(ymd_hms(ev[[date_col_ev]][bad]))
ev[, date := as.IDate(d_e)]
ev <- ev[!is.na(date)]

# Clean defendant names and flag commercial
ev[, clean_defendant_name := clean_name(defendant)]
ev[, commercial_alt := str_detect(clean_defendant_name, business_regex)]

# Deduplicate and filter
ev[, dup := .N, by = .(n_sn_ss_c, plaintiff, clean_defendant_name, d_filing)][, dup := dup > 1]
ev <- ev[
  commercial == "f" &
  commercial_alt == FALSE &
  dup == FALSE &
  year %in% 2006:2024 &
  total_rent <= 5e4 &
  !is.na(n_sn_ss_c)
]

ev[, year_quarter := to_yq(date)]

# Spatial join to hexes
ev_sf <- st_as_sf(ev, coords = c(lon_col_ev, lat_col_ev), crs = 4326, remove = FALSE)
ev_in_hex <- st_join(ev_sf, hex_sf[, "h3"], left = FALSE, join = st_within)
ev_dt <- as.data.table(st_drop_geometry(ev_in_hex))
ev_counts <- ev_dt[, .(eviction_count = .N), by = .(h3, year_quarter)]

logf("  Eviction counts aggregated to ", nrow(ev_counts), " hex-quarter combinations", log_file = log_file)

# ---- Build hex × YQ grid and assemble panel ----
logf("Building panel...", log_file = log_file)

yq_all <- sort(unique(c(crime_counts$year_quarter, ev_counts$year_quarter)))
hex_yq <- CJ(h3 = hex_ids, year_quarter = yq_all)

panel <- Reduce(
  function(a, b) merge(a, b, by = c("h3", "year_quarter"), all.x = TRUE),
  list(hex_yq, crime_counts, ev_counts)
)

# Filter to 2006-2024
panel <- panel[as.integer(substr(year_quarter, 1, 4)) %in% 2006:2024]

# Zero-fill counts
count_cols <- c(names(panel)[grepl("crime_count", names(panel))], "eviction_count")
for (cn in count_cols) {
  if (cn %in% names(panel)) panel[is.na(get(cn)), (cn) := 0L]
}

logf("  Panel rows: ", nrow(panel), log_file = log_file)
logf("  Unique hexes: ", uniqueN(panel$h3), log_file = log_file)
logf("  Unique year-quarters: ", uniqueN(panel$year_quarter), log_file = log_file)

# ---- Assertions ----
logf("Running assertions...", log_file = log_file)

# Check primary key uniqueness
n_rows <- nrow(panel)
n_unique <- uniqueN(panel, by = c("h3", "year_quarter"))
if (n_rows != n_unique) {
  stop("ASSERTION FAILED: Panel not unique on (h3, year_quarter)")
}
logf("  Unique on (h3, year_quarter): OK", log_file = log_file)

# ---- Write output ----
logf("Writing output...", log_file = log_file)

out_key <- paste0("hex_panel_res", RES)
out_path <- p_product(cfg, out_key)
fwrite(panel, out_path)

logf("  Wrote ", nrow(panel), " rows to ", out_path, log_file = log_file)
logf("=== Finished assign-evictions-crimes-homes-to-hexagon.r ===", log_file = log_file)
