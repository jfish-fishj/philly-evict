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

# ---- Config: paths / columns ----
RES <- 9
HEX_PATH <- sprintf("~/Desktop/data/philly-evict/philly_hex_res%d/phl_hex_res%d.shp", RES, RES)

# Point data (must have lat/long and date-ish field)
CRIME_PATH     <- "~/Desktop/data/philly-evict/philly-crime.csv"          # columns: latitude, longitude, date
EVICT_PATH     <- "~/Desktop/data/philly-evict/evict_address_cleaned.csv"      # columns: latitude, longitude, filedate (or similar)
HOMES_PATH     <- "inputs/philly_sales.csv"          # columns: parcel_lat, parcel_lon, sale_date, sale_amount + chars

# Column names (edit here once to match your raw files)
lat_col_crime <- "lat";  lon_col_crime <- "lng"; date_col_crime <- "dispatch_date"
lat_col_ev    <- "latitude";  lon_col_ev    <- "longitude"; date_col_ev    <- "d_filing"
lat_col_home  <- "parcel_lat"; lon_col_home <- "parcel_lon"; date_col_home <- "sale_date"
price_col     <- "sale_amount"

# Optional property characteristics (residualization-ready later if you want)
char_cols_base <- c("land_square_footage","universal_building_square_feet","year_built")

# Output
OUT_PANEL <- sprintf("output/phl_hex_yq_panel_res%d.csv", RES)

# ---- Helpers ----
# Comprehensive list of business entity terms (with abbreviations, plurals, and variations)
source("r/helper-functions.R")


ensure_wgs84 <- function(g) {
  if (is.na(sf::st_crs(g))) sf::st_set_crs(g, 4326)
  else if (sf::st_crs(g)$epsg != 4326) sf::st_transform(g, 4326) else g
}

to_yq <- function(d) {
  d <- as.IDate(d); sprintf("%d_Q%d", year(d), quarter(d))
}

# Exact ring counts (ring_0..ring_N) using h3jsr::get_ring; adapted from your prior structure
ring_counts_one_quarter <- function(h3_vec, counts_vec, N = 3) {
  stopifnot(length(h3_vec) == length(counts_vec))
  dt <- data.table(h3 = h3_vec, ring_0 = counts_vec)
  lut <- copy(dt); setnames(lut, "ring_0", "val"); setkey(lut, h3)
  for (k in seq_len(N)) {
    rings <- h3jsr::get_ring(h3_vec, ring_size = k, simple = TRUE)
    nb_dt <- data.table(
      h3_center = rep(h3_vec, lengths(rings)),
      h3_nb     = unlist(rings, use.names = FALSE)
    )
    nb_dt <- nb_dt[lut, on = .(h3_nb = h3), nomatch = 0L]
    ring_sum <- nb_dt[, .(v = sum(val, na.rm = TRUE)), by = h3_center]
    setnames(ring_sum, c("h3_center","v"), c("h3", paste0("ring_", k)))
    dt <- ring_sum[dt, on = "h3"]
    set(dt, which(is.na(dt[[paste0("ring_", k)]])), paste0("ring_", k), 0)
  }
  dt[]
}
# (This mirrors the approach in your existing script for ring creation and later joins.) :contentReference[oaicite:5]{index=5}

add_ring_leads_lags <- function(dt, ring_prefix = "crime_ring_", max_lag = 8, max_lead = 4) {
  setDT(dt)
  yq <- dt[, tstrsplit(year_quarter, "_Q", fixed = TRUE)]
  dt[, `:=`(.__Y__ = as.integer(yq[[1]]), .__Q__ = as.integer(yq[[2]]))]
  dt[, .__T__ := (.__Y__ * 4L + .__Q__)]
  ring_cols <- grep(paste0("^", ring_prefix), names(dt), value = TRUE)
  setkey(dt, h3, .__T__)
  for (col in ring_cols) {
    for (L in seq_len(max_lag))  dt[, paste0(col, "_lag_",  L) := shift(get(col), n = L, type = "lag"),  by = h3]
    for (F in seq_len(max_lead)) dt[, paste0(col, "_lead_", F) := shift(get(col), n = F, type = "lead"), by = h3]
  }
  dt[, c(".__Y__", ".__Q__", ".__T__") := NULL][]
}

# ---- Load hexes ----
hex_sf <- st_read(HEX_PATH, quiet = TRUE) |> ensure_wgs84()
stopifnot("h3" %in% names(hex_sf))
hex_ids <- hex_sf$h3

# ---- Load & tag CRIMES ----
crime <- fread(CRIME_PATH, showProgress = FALSE)
crime <- crime[!is.na(get(lat_col_crime)) & !is.na(get(lon_col_crime))]
# filter down further for just murders assaults robberies rapes
# crime = crime[str_detect(
#   text_general_code, regex("homicide|agg.+assault|robbery|rape", ignore_case = TRUE)
# )]

# DT: data.table with column text_general_code
violent  <- c("Homicide - Criminal","Homicide - Justifiable","Homicide - Gross Negligence",
              "Rape","Robbery Firearm","Robbery No Firearm",
              "Aggravated Assault Firearm","Aggravated Assault No Firearm")

property <- c("Thefts","Theft from Vehicle","Motor Vehicle Theft","Receiving Stolen Property",
              "Vandalism/Criminal Mischief","Arson","Fraud",
              "Forgery and Counterfeiting","Embezzlement")

nuisance <- c("Disorderly Conduct","Public Drunkenness","DRIVING UNDER THE INFLUENCE",
              "Liquor Law Violations","Vagrancy/Loitering","Gambling Violations",
              "Prostitution and Commercialized Vice","Narcotic / Drug Law Violations",
              "Weapon Violations")

crime[, crime_cat :=
     fcase(
       text_general_code %chin% violent,                     "Violent",
       text_general_code == "Other Assaults",                "Simple Assault",
       grepl("^Burglary", text_general_code),                "Burglary",
       text_general_code %chin% property,                    "Property",
       text_general_code %chin% nuisance,                    "Nuisance",
       default = "Other"
     )
]

# robust date parse
d_c <- suppressWarnings(ymd(crime[[date_col_crime]]))
bad <- is.na(d_c); if (any(bad)) d_c[bad] <- suppressWarnings(ymd_hms(crime[[date_col_crime]][bad]))
crime[, date := as.IDate(d_c)]
crime <- crime[!is.na(date)]
crime[, year_quarter := to_yq(date)]
crime_sf <- st_as_sf(crime, coords = c(lon_col_crime, lat_col_crime), crs = 4326, remove = FALSE)
crime_in_hex <- st_join(crime_sf, hex_sf[, "h3"], left = FALSE, join = st_within)
crime_dt <- as.data.table(st_drop_geometry(crime_in_hex))
# count crimes by type
# first assign each crime a general category;
# violet; property
crime_counts_long <- crime_dt[, .(crime_count = .N), by = .(h3, year_quarter, crime_cat)]
crime_counts = dcast(crime_counts_long, h3 + year_quarter ~ crime_cat, value.var = "crime_count", fill = 0)
# rename all to be crime_count_{category}
setnames(crime_counts,
         old = setdiff(names(crime_counts), c("h3", "year_quarter")),
         new = paste0("crime_count_", setdiff(names(crime_counts), c("h3", "year_quarter")))
)

# ---- Load & tag EVICTIONS ----
ev <- fread(EVICT_PATH, showProgress = FALSE)
ev <- ev[!is.na(get(lat_col_ev)) & !is.na(get(lon_col_ev))]
d_e <- suppressWarnings(ymd(ev[[date_col_ev]]))
bad <- is.na(d_e); if (any(bad)) d_e[bad] <- suppressWarnings(ymd_hms(ev[[date_col_ev]][bad]))
ev[, date := as.IDate(d_e)]
ev <- ev[!is.na(date)]
ev[,clean_defendant_name := clean_name(defendant)]
# flag commercial
ev[,commercial_alt := str_detect(clean_defendant_name, business_regex)]

ev[,dup := .N, by = .(n_sn_ss_c, plaintiff,clean_defendant_name,d_filing)][,dup := dup > 1]
ev = ev[commercial == "f" &
          commercial_alt == F &
          dup == F &
          year%in% 2006:2024 &
          total_rent <= 5e4 &
          !is.na(n_sn_ss_c)]
ev[, year_quarter := to_yq(date)]
ev_sf <- st_as_sf(ev, coords = c(lon_col_ev, lat_col_ev), crs = 4326, remove = FALSE)
ev_in_hex <- st_join(ev_sf, hex_sf[, "h3"], left = FALSE, join = st_within)
ev_dt <- as.data.table(st_drop_geometry(ev_in_hex))
ev_counts <- ev_dt[, .(eviction_count = .N), by = .(h3, year_quarter)]

# ---- Load & tag HOMES / SALES ----
# homes <- fread(HOMES_PATH, showProgress = FALSE)
# homes <- homes[!is.na(get(lat_col_home)) & !is.na(get(lon_col_home))]
# d_h <- suppressWarnings(ymd(homes[[date_col_home]]))
# bad <- is.na(d_h); if (any(bad)) d_h[bad] <- suppressWarnings(ymd_hms(homes[[date_col_home]][bad]))
# homes[, date := as.IDate(d_h)]
# homes <- homes[!is.na(date)]
# homes[, year_quarter := to_yq(date)]
# # price guard
# if (price_col %in% names(homes)) homes <- homes[get(price_col) > 0]
#
# homes_sf <- st_as_sf(homes, coords = c(lon_col_home, lat_col_home), crs = 4326, remove = FALSE)
# homes_in_hex <- st_join(homes_sf, hex_sf[, "h3"], left = FALSE, join = st_within)
# homes_dt <- as.data.table(st_drop_geometry(homes_in_hex))
#
# sales_stats <- if (price_col %in% names(homes_dt)) {
#   homes_dt[, .(
#     n_sales = .N,
#     avg_log_price = mean(log(get(price_col)), na.rm = TRUE)
#   ), by = .(h3, year_quarter)]
# } else {
#   homes_dt[, .(n_sales = .N), by = .(h3, year_quarter)]
# }

# ---- Build hex × YQ grid and assemble panel ----
yq_all <- sort(unique(c(crime_counts$year_quarter, ev_counts$year_quarter#, sales_stats$year_quarter
                        )))
hex_yq <- CJ(h3 = hex_ids, year_quarter = yq_all)

panel <- Reduce(function(a, b) merge(a, b, by = c("h3", "year_quarter"), all.x = TRUE),
                list(hex_yq, crime_counts, ev_counts))
# just 2006-2024
panel <- panel[as.integer(substr(year_quarter, 1, 4)) %in% 2006:2024]
# zero-fill counts
for (cn in c(panel %>% select(contains("crime_count")) %>% colnames(), "eviction_count"#, "n_sales"
             )) {
  if (cn %in% names(panel)) panel[is.na(get(cn)), (cn) := 0L]
}

# # ---- Crime rings per YQ (0..3), then leads/lags ----
# N_RINGS <- 3
# ring_list <- vector("list", length(yq_all))
# for (i in seq_along(yq_all)) {
#   q <- yq_all[i]
#   sub <- panel[year_quarter == q, .(h3, crime_count)]
#   if (!nrow(sub)) next
#   rc <- ring_counts_one_quarter(sub$h3, as.numeric(sub$crime_count), N = N_RINGS)
#   rc[, year_quarter := q]
#   setnames(rc, paste0("ring_", 0:N_RINGS), paste0("crime_ring_", 0:N_RINGS))
#   ring_list[[i]] <- rc
# }
# rings_dt <- rbindlist(ring_list, use.names = TRUE, fill = TRUE)
# if (nrow(rings_dt)) panel <- merge(panel, rings_dt, by = c("h3", "year_quarter"), all.x = TRUE)
#
# # Sanity: ring_0 equals crime_count (allowing for NAs) :contentReference[oaicite:6]{index=6}
# if ("crime_ring_0" %in% names(panel)) {
#   diff_abs <- sum(abs(fifelse(is.na(panel$crime_ring_0), 0, panel$crime_ring_0) - fifelse(is.na(panel$crime_count), 0, panel$crime_count)))
#   if (diff_abs != 0) message(sprintf("[warn] ring_0 vs crime_count abs diff = %s", format(diff_abs, big.mark=",")))
# }
#
# # Leads/Lags on crime rings (same method you were using) :contentReference[oaicite:7]{index=7}
# panel <- add_ring_leads_lags(panel, max_lag = 8, max_lead = 4)

# ---- Export ----
fwrite(panel, OUT_PANEL)
message(sprintf("[OK] Philly hex×YQ panel → %s  (rows=%s, hexes=%s, YQs=%s)",
                OUT_PANEL,
                format(nrow(panel), big.mark=","),
                format(length(unique(panel$h3)), big.mark=","),
                format(length(unique(panel$year_quarter)), big.mark=",")))
