## ============================================================
## make_bldg_pid_xwalk.r
## ============================================================
## Purpose: Create a crosswalk between building footprints and parcels (PIDs)
##          by spatial intersection. For each building, find the parcel with
##          the greatest overlap area. Then aggregate to parcel-level stats.
##
## Inputs (from config):
##   - philly_parcels_sf: Parcel shapefile (DOR_Parcel.shp)
##   - building_footprints_shp: Building footprints shapefile
##   - parcel_building_2024: Parcel-building CSV (for PID-PIN mapping)
##
## Outputs (to config):
##   - building_pid_xwalk: Parcel-level summary (PID, num_bldgs, mean_height, mean_sqft)
##   - parcel_building_summary: Building-level matched data (PID, address, max_hgt, square_ft)
##
## Primary keys:
##   - building_pid_xwalk: PID
##   - parcel_building_summary: (building footprint row, one-to-one with matched parcel)
## ============================================================

suppressPackageStartupMessages({
  library(sf)
  library(data.table)
  library(dplyr)
  library(purrr)
  library(lwgeom)
  library(geos)
})

# ---- Load config and set up logging ----
# ---- Load config and set up logging ----
source("r/config.R")
source("R/helper-functions.R")

normalize_pid <- function(x) {
  stringr::str_pad(as.character(x), width = 9, side = "left", pad = "0")
}


cfg <- read_config()
log_file <- p_out(cfg, "logs", "make_bldg_pid_xwalk.log")

logf("=== Starting make_bldg_pid_xwalk.r ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ---- Configuration parameters ----
ea_crs <- 6933                # World Cylindrical Equal Area (meters)
max_matches_per_A <- 10L      # Buildings with more than this many parcel candidates are marked invalid

logf("Parameters: ea_crs=", ea_crs, ", max_matches_per_A=", max_matches_per_A, log_file = log_file)

# ---- Load input data ----
logf("Loading input data...", log_file = log_file)

# Parcel shapefile
parcel_shp_path <- p_input(cfg, "philly_parcels_sf")
logf("  Reading parcel shapefile: ", parcel_shp_path, log_file = log_file)
philly_sf <- st_read(parcel_shp_path, quiet = TRUE)
logf("  Parcel shapefile: ", nrow(philly_sf), " features", log_file = log_file)

# Building footprints shapefile
bldg_shp_path <- p_input(cfg, "building_footprints_shp")
logf("  Reading building footprints shapefile: ", bldg_shp_path, log_file = log_file)
philly_bldgs_sf <- st_read(bldg_shp_path, quiet = TRUE)
logf("  Building footprints: ", nrow(philly_bldgs_sf), " features", log_file = log_file)

# Parcel-building CSV for PID-PIN mapping
parcel_csv_path <- p_product(cfg, "parcels_clean")
logf("  Reading parcel CSV for PID-PIN mapping: ", parcel_csv_path, log_file = log_file)
philly_parcels <- fread(parcel_csv_path)
logf("  Parcel CSV: ", nrow(philly_parcels), " rows", log_file = log_file)

# make PID

# Merge PIN to get PID on the shapefile
n_before <- nrow(philly_sf)
philly_parcels[,PID := normalize_pid(parcel_number)]
philly_sf <- merge(philly_sf, philly_parcels[, .(PID, PIN = pin)], by = "PIN", all.x = FALSE)
logf("  After PID merge: ", nrow(philly_sf), " parcels (dropped ", n_before - nrow(philly_sf), " without PID)", log_file = log_file)

# ---- Spatial prep ----
logf("Preparing spatial data...", log_file = log_file)

# Assign working variables (A = buildings, B = parcels)
A0 <- philly_bldgs_sf
B0 <- philly_sf

# Validate CRS
stopifnot(!is.na(st_crs(A0)), !is.na(st_crs(B0)))
if (st_crs(A0) != st_crs(B0)) {
  logf("  Transforming parcels to building CRS...", log_file = log_file)
  B0 <- st_transform(B0, st_crs(A0))
}

# Make geometries valid
logf("  Making geometries valid...", log_file = log_file)
A0 <- st_make_valid(A0)
B0 <- st_make_valid(B0)

# Track internal IDs
A0$.__aid__ <- seq_len(nrow(A0))
B0$.__bid__ <- seq_len(nrow(B0))

# Project to equal-area CRS for area calculations
logf("  Projecting to equal-area CRS (EPSG:", ea_crs, ")...", log_file = log_file)
A_ea <- st_transform(A0, ea_crs)
B_ea <- st_transform(B0, ea_crs)

# Snap to grid to reduce slivers
A_ea <- st_set_precision(A_ea, 1) |> st_snap_to_grid(1) |> st_make_valid()
B_ea <- st_set_precision(B_ea, 1) |> st_snap_to_grid(1) |> st_make_valid()

# ---- Candidate generation via spatial index ----
logf("Building intersection candidates via spatial index...", log_file = log_file)
cand <- st_intersects(A_ea, B_ea, sparse = TRUE)

k <- lengths(cand)
invalid_aids <- which(k > max_matches_per_A)
valid_aids   <- which(k > 0L & k <= max_matches_per_A)
zero_aids    <- which(k == 0L)

logf("  Buildings with >", max_matches_per_A, " matches (dropped): ", length(invalid_aids), log_file = log_file)
logf("  Buildings with 1..", max_matches_per_A, " matches (kept): ", length(valid_aids), log_file = log_file)
logf("  Buildings with 0 matches: ", length(zero_aids), log_file = log_file)

# Build pair list for valid buildings
aid <- rep.int(valid_aids, k[valid_aids])
bid <- unlist(cand[valid_aids], use.names = FALSE)
pairs <- data.table(.__aid__ = aid, .__bid__ = bid)

# Split singletons vs ambiguous
counts <- pairs[, .N, by = .__aid__]
single_aids <- counts[N == 1L, .__aid__]
ambig_aids  <- counts[N > 1L, .__aid__]

logf("  Singleton matches: ", length(single_aids), log_file = log_file)
logf("  Ambiguous matches (need area calculation): ", length(ambig_aids), log_file = log_file)

# Singletons: trivial winners
single_winners <- pairs[.__aid__ %in% single_aids, .(.__aid__, .__bid__)]
single_winners[, overlap_m2 := NA_real_]

# ---- Resolve ambiguous matches by overlap area ----
ambig_pairs <- pairs[.__aid__ %in% ambig_aids]
use_geos <- requireNamespace("geos", quietly = TRUE)

if (nrow(ambig_pairs) > 0L) {
  logf("Resolving ", length(ambig_aids), " ambiguous buildings across ", nrow(ambig_pairs), " pairs",
       if (use_geos) " with geos" else " with sf fallback", "...", log_file = log_file)
}

if (nrow(ambig_pairs) > 0L && use_geos) {
  # Fast path: geos element-wise intersection
  idxA <- ambig_pairs$.__aid__
  idxB <- ambig_pairs$.__bid__

  gA <- geos::geos_read_wkb(st_as_binary(st_geometry(A_ea)[idxA], EWKB = TRUE))
  gB <- geos::geos_read_wkb(st_as_binary(st_geometry(B_ea)[idxB], EWKB = TRUE))

  gI    <- geos::geos_intersection(gA, gB)
  areas <- geos::geos_area(gI)

  ambig_pairs[, overlap_m2 := as.numeric(areas)]
  ambig_pairs <- ambig_pairs[is.finite(overlap_m2) & overlap_m2 > 0]

  # Winner per building (max overlap)
  ambig_winners <- ambig_pairs[
    ambig_pairs[, .I[which.max(overlap_m2)], by = .__aid__]$V1,
  ][, .(.__aid__, .__bid__, overlap_m2)]

} else if (nrow(ambig_pairs) > 0L) {
  # Fallback: sf overlay
  A_sub <- A_ea[ambig_pairs$.__aid__, c(".__aid__")]
  B_sub <- B_ea[ambig_pairs$.__bid__, c(".__bid__")]

  ov <- suppressWarnings(st_intersection(A_sub, B_sub))
  ov$overlap_m2 <- as.numeric(st_area(ov))

  pair_areas <- as.data.table(st_drop_geometry(ov))[
    , .(overlap_m2 = sum(overlap_m2, na.rm = TRUE)), by = .(.__aid__, .__bid__)
  ]

  ambig_pairs <- merge(ambig_pairs, pair_areas, by = c(".__aid__", ".__bid__"), all.x = TRUE)
  ambig_pairs <- ambig_pairs[is.finite(overlap_m2) & overlap_m2 > 0]

  ambig_winners <- ambig_pairs[
    ambig_pairs[, .I[which.max(overlap_m2)], by = .__aid__]$V1,
  ][, .(.__aid__, .__bid__, overlap_m2)]
} else {
  ambig_winners <- data.table(.__aid__ = integer(), .__bid__ = integer(), overlap_m2 = double())
}

# ---- Build invalid and zero-match rows ----
invalid_rows <- data.table(
  .__aid__ = invalid_aids,
  .__bid__ = NA_integer_,
  overlap_m2 = NA_real_,
  invalid_many_matches = TRUE
)
zero_rows <- data.table(
  .__aid__ = zero_aids,
  .__bid__ = NA_integer_,
  overlap_m2 = NA_real_,
  invalid_many_matches = FALSE
)

# ---- Combine all winners ----
winners <- rbindlist(list(
  data.table(single_winners, invalid_many_matches = FALSE),
  data.table(ambig_winners, invalid_many_matches = FALSE),
  invalid_rows,
  zero_rows
), use.names = TRUE, fill = TRUE)

# Deduplicate (shouldn't happen, but safety)
setorder(winners, .__aid__, -overlap_m2, na.last = TRUE)
winners <- winners[!duplicated(.__aid__)]

logf("Total buildings processed: ", nrow(winners), log_file = log_file)

# ---- Join back to original building data ----
logf("Joining attributes back to buildings...", log_file = log_file)

A_matched <- A0 |>
  st_drop_geometry() |>
  left_join(as_tibble(winners), by = ".__aid__") |>
  left_join(B0 |> st_drop_geometry() |> select(.__bid__, dplyr::everything()), by = ".__bid__") |>
  left_join(A0 |> select(.__aid__, geometry), by = ".__aid__") |>
  st_as_sf(crs = st_crs(A0))

# ---- QA checks ----
n_matched <- sum(!is.na(A_matched$.__bid__))
n_invalid <- sum(A_matched$invalid_many_matches %in% TRUE)
n_zero    <- sum(is.na(A_matched$.__bid__) & !A_matched$invalid_many_matches %in% TRUE)

logf("QA checks:", log_file = log_file)
logf("  Matched buildings (non-NA PID): ", n_matched, log_file = log_file)
logf("  Invalid (too many matches): ", n_invalid, log_file = log_file)
logf("  Zero-match buildings: ", n_zero, log_file = log_file)

# ---- Aggregate to parcel level ----
logf("Aggregating to parcel level...", log_file = log_file)

A_matched_dt <- as.data.table(A_matched)

PID_xwalk <- A_matched_dt[!is.na(PID), .(
  num_bldgs = .N,
  mean_height = mean(max_hgt, na.rm = TRUE),
  mean_sqft = mean(square_ft, na.rm = TRUE)
), by = .(PID)]

logf("  Unique PIDs with buildings: ", nrow(PID_xwalk), log_file = log_file)
logf("  Buildings per parcel distribution:", log_file = log_file)
logf("    Min: ", min(PID_xwalk$num_bldgs), ", Median: ", median(PID_xwalk$num_bldgs),
     ", Max: ", max(PID_xwalk$num_bldgs), log_file = log_file)

# Log distribution of num_bldgs
bldg_dist <- PID_xwalk[, .N, by = .(num_bldgs)][order(num_bldgs)]
bldg_dist[, binned := cut(num_bldgs,
  breaks = c(-1, 0, 1, 2, 3, 5, 10, 20, 50, 100, 200, 500, 1000, Inf),
  labels = c("0", "1", "2", "3", "4-5", "6-10", "11-20", "21-50", "51-100", "101-200", "201-500", "501-1000", "1000+")
)]
bldg_summary <- bldg_dist[, .(parcels = sum(N)), by = binned][order(binned)]
logf("  Buildings per parcel binned:", log_file = log_file)
for (i in seq_len(nrow(bldg_summary))) {
  logf("    ", bldg_summary$binned[i], ": ", bldg_summary$parcels[i], " parcels", log_file = log_file)
}

# ---- Prepare building-level summary ----
parcel_bldg_summary <- A_matched_dt[!is.na(PID), .(PID, address, max_hgt, square_ft)]
logf("  Building-level summary rows: ", nrow(parcel_bldg_summary), log_file = log_file)

# ---- Write outputs ----
logf("Writing outputs...", log_file = log_file)

out_xwalk <- p_product(cfg, "building_pid_xwalk")
fwrite(PID_xwalk, out_xwalk)
logf("  Wrote: ", out_xwalk, " (", nrow(PID_xwalk), " rows)", log_file = log_file)

out_summary <- p_product(cfg, "parcel_building_summary")
fwrite(parcel_bldg_summary, out_summary)
logf("  Wrote: ", out_summary, " (", nrow(parcel_bldg_summary), " rows)", log_file = log_file)

logf("=== Completed make_bldg_pid_xwalk.r ===", log_file = log_file)
