## ============================================================
## make-hexagons.r
## ============================================================
## Purpose: Create H3 hexagon grid for Philadelphia
##
## Inputs:
##   - cfg$inputs$philly_bg_shp (shapefiles/philly_bg.shp)
##
## Outputs:
##   - Hex grid shapefiles at specified resolution (shapefiles/philly_hex_res{RES})
##
## Primary key: h3 (H3 index)
## ============================================================

# ---- Threads / libs ----
Sys.setenv("OMP_THREAD_LIMIT" = max(1, parallel::detectCores() %/% 2))
data.table::setDTthreads(max(1, parallel::detectCores() %/% 2))

suppressPackageStartupMessages({
  library(sf)
  library(data.table)
  library(h3jsr)      # polygon_to_cells, cell_to_polygon
})

# ---- Load config and set up logging ----
source("r/config.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "make-hexagons.log")

logf("=== Starting make-hexagons.r ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ---- Config ----
RES <- cfg$spatial$h3_resolution %||% 9
logf("H3 resolution: ", RES, log_file = log_file)

# Input: Philadelphia boundary shapefile
PHL_SHP <- p_input(cfg, "philly_bg_shp")

# Output directory for hex grid
OUT_DIR <- p_in(cfg, sprintf("shapefiles/philly_hex_res%d", RES))
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

OUT_HEX_SHP     <- file.path(OUT_DIR, sprintf("phl_hex_res%d.shp", RES))
OUT_HEX_GPKG    <- file.path(OUT_DIR, sprintf("phl_hex_res%d.gpkg", RES))
OUT_HEX_PARQUET <- file.path(OUT_DIR, sprintf("phl_hex_res%d.parquet", RES))

# ---- Load & dissolve Philadelphia outline (WGS84) ----
phl <- st_read(PHL_SHP, quiet = TRUE)
if (is.na(st_crs(phl))) stop("Philadelphia shapefile has no CRS. Set a CRS (e.g., EPSG:4269 or 4326).")
phl <- st_transform(phl, 4326)

# dissolve to a single polygon
if (!"__one__" %in% names(phl)) phl$`__one__` <- 1L
phl_diss <- phl |>
  dplyr::group_by(`__one__`) |>
  dplyr::summarise(.groups = "drop") |>
  st_make_valid()

ggplot2::ggplot() +
  ggplot2::geom_sf(data = phl_diss, fill = "lightblue", color = "black") +
  ggplot2::labs(title = "Philadelphia Boundary") +
  ggplot2::theme_minimal()

# ---- H3 polyfill at RES=8 ----
# This uses the library’s polygon-to-cells instead of manual flood-fill
# (it’s the R analog of your BFS/neighbors approach in Python) :contentReference[oaicite:2]{index=2}
h3_idx <- polygon_to_cells(phl_diss, res = RES, simple = T)  # character[] of h3 indexes

# build hex polygons (sf)
hex_sf <- cell_to_polygon(h3_idx, simple = FALSE) # MULTIPOLYGON per h3
hex_sf$h3 <- hex_sf$h3_address
hex_sf <- st_set_crs(hex_sf, 4326)
hex_sf <- hex_sf[, c("h3", "geometry")]

ggplot2::ggplot() +
  ggplot2::geom_sf(data = hex_sf, fill = "lightblue", color = "black") +
  ggplot2::labs(title = "Philadelphia Boundary (hexagons)") +
  ggplot2::theme_minimal()

# ---- Export ----
logf("Writing outputs...", log_file = log_file)

# shapefile
st_write(hex_sf, OUT_HEX_SHP, quiet = TRUE, delete_layer = TRUE)
logf("  Wrote shapefile: ", OUT_HEX_SHP, log_file = log_file)

# gpkg
try(st_write(hex_sf, OUT_HEX_GPKG, quiet = TRUE, delete_layer = TRUE), silent = TRUE)
logf("  Wrote geopackage: ", OUT_HEX_GPKG, log_file = log_file)

# parquet (requires sf >= 1.0 w/ arrow; skip if not available)
if (requireNamespace("arrow", quietly = TRUE)) {
  try(st_write_parquet(hex_sf, OUT_HEX_PARQUET), silent = TRUE)
  logf("  Wrote parquet: ", OUT_HEX_PARQUET, log_file = log_file)
}

logf("  Hex count: ", nrow(hex_sf), " at resolution ", RES, log_file = log_file)
logf("=== Finished make-hexagons.r ===", log_file = log_file)
