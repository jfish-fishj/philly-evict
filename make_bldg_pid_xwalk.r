
# Packages
library(sf)
library(dplyr)
library(purrr)
library(lwgeom)   # for st_geod_area on lon/lat
library(geos)
# --- Inputs (edit these) ---

philly_sf =read_sf("~/Desktop/data/philly-evict/philly_parcels_sf/DOR_Parcel.shp")
philly_bldgs_sf = read_sf("~/Desktop/data/philly-evict/LI_BUILDING_FOOTPRINTS/LI_BUILDING_FOOTPRINTS.shp")
philly_parcels = fread("~/Desktop/data/philly-evict/processed/parcel_building_2024.csv")
philly_sf = merge(philly_sf,philly_parcels[,.(PID, PIN = pin)], by = "PIN", all.x=F)

ea_crs <- 6933                # World Cylindrical Equal Area (meters); swap to a regional EA CRS if desired
max_matches_per_A <- 10L      # A’s with more than this many candidate B’s are marked invalid and skipped

# --------- READ & PREP -----------
message("Reading shapefiles...")
B0 <- philly_sf
A0 <- philly_bldgs_sf

stopifnot(!is.na(st_crs(A0)), !is.na(st_crs(B0)))
if (st_crs(A0) != st_crs(B0)) {
  message("Transforming B to A's CRS...")
  B0 <- st_transform(B0, st_crs(A0))
}

# Make valid (avoid topology errors)
A0 <- st_make_valid(A0)
B0 <- st_make_valid(B0)

# Track ids
A0$.__aid__ <- seq_len(nrow(A0))
B0$.__bid__ <- seq_len(nrow(B0))

# Work copies in equal-area CRS for robust, fast area computation
message("Projecting to equal-area CRS for area calculations...")
A_ea <- st_transform(A0, ea_crs)
B_ea <- st_transform(B0, ea_crs)

# Optional: snap-to-grid to reduce slivers (tweak precision as needed; units = meters in many EA CRSs)
A_ea <- st_set_precision(A_ea, 1) |> st_snap_to_grid(1) |> st_make_valid()
B_ea <- st_set_precision(B_ea, 1) |> st_snap_to_grid(1) |> st_make_valid()

# --------- CANDIDATE GENERATION ----------
message("Building intersection candidates via spatial index...")
cand <- st_intersects(A_ea, B_ea, sparse = TRUE)   # list of integer vectors

k <- lengths(cand)
invalid_aids <- which(k > max_matches_per_A)
valid_aids   <- which(k > 0L & k <= max_matches_per_A)
zero_aids    <- which(k == 0L)

message(sprintf("A with >%d matches (dropped): %d", max_matches_per_A, length(invalid_aids)))
message(sprintf("A with 1..%d matches (kept): %d", max_matches_per_A, length(valid_aids)))
message(sprintf("A with 0 matches: %d", length(zero_aids)))

# Build pair list only for valid A’s (vectorized, fast)
aid <- rep.int(valid_aids, k[valid_aids])
bid <- unlist(cand[valid_aids], use.names = FALSE)
pairs <- data.table(.__aid__ = aid, .__bid__ = bid)

# Split singletons vs ambiguous
counts <- pairs[, .N, by = .__aid__]
single_aids <- counts[N == 1L, .__aid__]
ambig_aids  <- counts[N >  1L, .__aid__]

# Singletons: trivial winners (no area math)
single_winners <- pairs[.__aid__ %in% single_aids, .(.__aid__, .__bid__)]
single_winners[, overlap_m2 := NA_real_]

# --------- AMBIGUOUS: PAIRWISE INTERSECTIONS ----------
ambig_pairs <- pairs[.__aid__ %in% ambig_aids]

use_geos <- requireNamespace("geos", quietly = TRUE)

if (nrow(ambig_pairs) > 0L) {
  message(sprintf("Resolving %d ambiguous A's across %d pairs%s...",
                  length(ambig_aids), nrow(ambig_pairs),
                  if (use_geos) " with geos" else " with sf-aggregate fallback"))
}

if (nrow(ambig_pairs) > 0L && use_geos) {
  # --------- Fast path: geos (element-wise, 1:1) ---------
  # geos works on WKB; ensure we pass sfc, aligned 1:1
  idxA <- ambig_pairs$.__aid__
  idxB <- ambig_pairs$.__bid__

  gA <- geos::geos_read_wkb(st_as_binary(st_geometry(A_ea)[idxA], EWKB = TRUE))
  gB <- geos::geos_read_wkb(st_as_binary(st_geometry(B_ea)[idxB], EWKB = TRUE))

  # Pairwise intersection, exact length equality guaranteed
  gI    <- geos::geos_intersection(gA, gB)
  areas <- geos::geos_area(gI)

  ambig_pairs[, overlap_m2 := as.numeric(areas)]
  # Keep positive area only
  ambig_pairs <- ambig_pairs[is.finite(overlap_m2) & overlap_m2 > 0]

  # Winner per A
  ambig_winners <- ambig_pairs[
    ambig_pairs[, .I[which.max(overlap_m2)], by = .__aid__]$V1,
  ][, .(.__aid__, .__bid__, overlap_m2)]

} else if (nrow(ambig_pairs) > 0L) {
  # --------- Fallback: sf overlay may explode rows; aggregate back to pair ---------
  # Build minimal sf rows with ids
  A_sub <- A_ea[ambig_pairs$.__aid__, c(".__aid__")]
  B_sub <- B_ea[ambig_pairs$.__bid__, c(".__bid__")]

  # Many-to-many overlay
  ov <- suppressWarnings(st_intersection(A_sub, B_sub))

  # Area then aggregate to (aid, bid)
  ov$overlap_m2 <- as.numeric(st_area(ov))
  pair_areas <- as.data.table(st_drop_geometry(ov))[
    , .(overlap_m2 = sum(overlap_m2, na.rm = TRUE)), by = .(.__aid__, .__bid__)
  ]

  # Attach to ambig_pairs (left join); then filter and reduce
  ambig_pairs <- merge(ambig_pairs, pair_areas, by = c(".__aid__", ".__bid__"), all.x = TRUE)
  ambig_pairs <- ambig_pairs[is.finite(overlap_m2) & overlap_m2 > 0]

  ambig_winners <- ambig_pairs[
    ambig_pairs[, .I[which.max(overlap_m2)], by = .__aid__]$V1,
  ][, .(.__aid__, .__bid__, overlap_m2)]
} else {
  ambig_winners <- data.table(.__aid__ = integer(), .__bid__ = integer(), overlap_m2 = double())
}

# --------- INVALID / ZERO-MATCH ROWS ----------
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

# --------- COMBINE WINNERS ----------
winners <- rbindlist(list(
  data.table(single_winners, invalid_many_matches = FALSE),
  data.table(ambig_winners,  invalid_many_matches = FALSE),
  invalid_rows,
  zero_rows
), use.names = TRUE, fill = TRUE)

# In case of duplicates (shouldn’t happen), keep the one with largest overlap
setorder(winners, .__aid__, -overlap_m2, na.last = TRUE)
winners <- winners[!duplicated(.__aid__)]

# --------- JOIN BACK TO ORIGINAL A (keep A geometry; add chosen B attrs) ----------
message("Joining attributes back to A...")
A_matched <- A0 |>
  st_drop_geometry() |>
  left_join(as_tibble(winners), by = ".__aid__") |>
  left_join(B0 |> st_drop_geometry() |> select(.__bid__, dplyr::everything()), by = ".__bid__") |>
  # Re-attach A geometry and CRS
  left_join(A0 |> select(.__aid__, geometry), by = ".__aid__") |>
  st_as_sf(crs = st_crs(A0))

# --------- OUTPUT & CHECKS ----------
message("Done. Quick checks:")
message(sprintf("Matched A (non-NA __bid__): %d", sum(!is.na(A_matched$.__bid__))))
message(sprintf("Invalid (many matches) A: %d", sum(A_matched$invalid_many_matches %in% TRUE)))
message(sprintf("Zero-match A: %d", sum(is.na(A_matched$.__bid__) & !A_matched$invalid_many_matches %in% TRUE)))

# convert to data.table and export
A_matched_dt = as.data.table(A_matched)
PID_xwalk = A_matched_dt[!is.na(PID),list(
  num_bldgs = .N,
  mean_height = mean(max_hgt, na.rm=TRUE),
  mean_sqft = mean(square_ft, na.rm=TRUE)
), by = .(PID)]

PID_xwalk[,quantile(num_bldgs)]
# bin into categories
PID_xwalk[,.N, by = (num_bldgs)][,
  binned := cut(num_bldgs, breaks = c(-1,0,1,2,3,5,10,20,50,100,200,500,1000,Inf),
               labels = c("0","1","2","3","4-5","6-10","11-20","21-50","51-100","101-200","201-500","501-1000","1000+"))
][,sum(N), by = binned][order(binned)]

fwrite(PID_xwalk, "~/Desktop/data/philly-evict/processed/building_pid_xwalk_2024.csv")
fwrite(A_matched_dt %>% select(PID, address, max_hgt, square_ft), "~/Desktop/data/philly-evict/processed/parcel_building_summary_2024.csv")


# Example write:
# st_write(A_matched, "A_matched.shp", delete_dsn = TRUE)
# st_write(A_matched, "A_matched.gpkg", delete_dsn = TRUE, layer = "A_matched")

# If you also want the chosen B geometry along with A geometry, do a separate join:
# B_chosen <- B0[match(A_matched$.__bid__, B0$.__bid__), c(".__bid__", "geometry")]
# names(B_chosen)[names(B_chosen) == "geometry"] <- "geometry_B"
# A_matched <- A_matched |> cbind(st_drop_geometry(B_chosen))
