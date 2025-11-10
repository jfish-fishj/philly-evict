# ---- Threads / libs ----
Sys.setenv("OMP_THREAD_LIMIT" = max(1, parallel::detectCores() %/% 2))
data.table::setDTthreads(max(1, parallel::detectCores() %/% 2))

suppressPackageStartupMessages({
  library(sf)
  library(data.table)
  library(h3jsr)      # polygon_to_cells, cell_to_polygon
})

# ---- Config ----
RES <- 9
# point to any polygon layer you can dissolve to the City of Philadelphia boundary
# e.g., a TIGER/Line place file already clipped to Philadelphia, or a county subset you can dissolve
PHL_SHP <- "~/Desktop/data/philly-evict/philly_bg.shp"

OUT_DIR <- sprintf("~/Desktop/data/philly-evict/philly_hex_res%d", RES)
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
# shapefile
st_write(hex_sf, OUT_HEX_SHP, quiet = TRUE)
# gpkg
try(st_write(hex_sf, OUT_HEX_GPKG, quiet = TRUE), silent = TRUE)
# parquet (requires sf >= 1.0 w/ arrow; skip if not available)
if (requireNamespace("arrow", quietly = TRUE)) {
  try(st_write_parquet(hex_sf, OUT_HEX_PARQUET), silent = TRUE)
}

message(sprintf("[OK] Philly hex grid → %s (n=%s @ res=%d)", OUT_HEX_SHP, format(nrow(hex_sf), big.mark=","), RES))
