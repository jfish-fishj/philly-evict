## ============================================================
## merge-infousa-parcels.R
## ============================================================
## Purpose: Merge InfoUSA household data with parcel records
##          Creates address crosswalk for linking InfoUSA to parcels
##
## Inputs:
##   - cfg$inputs$infousa_cleaned (infousa/infousa_address_cleaned.csv)
##   - cfg$products$parcels_clean (clean/parcels_clean.csv)
##   - cfg$inputs$philly_parcels_sf (shapefiles/philly_parcels_sf/DOR_Parcel.shp)
##
## Outputs:
##   - cfg$products$infousa_address_xwalk (xwalks/philly_infousa_dt_address_agg_xwalk.csv)
##   - cfg$products$infousa_address_agg (xwalks/philly_infousa_dt_address_agg.csv)
##
## Primary key: n_sn_ss_c (address key) for xwalk
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(sf)
  library(tidycensus)
})

# ---- Load config and set up logging ----
source("r/config.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "merge-infousa-parcels.log")

logf("=== Starting merge-infousa-parcels.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ---- Load input data ----
logf("Loading input data...", log_file = log_file)

infousa_path <- p_product(cfg, "infousa_clean")
if (!file.exists(infousa_path)) {
  infousa_path <- p_input(cfg, "infousa_cleaned")
}
logf("  InfoUSA source: ", infousa_path, log_file = log_file)
philly_infousa_dt <- fread(infousa_path)
philly_infousa_dt[, obs_id := .I]
philly_parcels <- fread(p_product(cfg, "parcels_clean"))
philly_parcels_sf <- read_sf(p_input(cfg, "philly_parcels_sf"))

logf("  InfoUSA: ", nrow(philly_infousa_dt), " rows", log_file = log_file)
logf("  Parcels: ", nrow(philly_parcels), " rows", log_file = log_file)



philly_parcels[,PID:= str_pad(parcel_number,9, "left",pad = "0")]
philly_parcels_sf_m = philly_parcels_sf %>% select(-matches("PID")) %>%
  merge(philly_parcels[,.(pin, PID)], by.x = "PIN", by.y = "pin")

# Normalize pm.zip for merging: strip "_" prefix, pad to 5 digits
# This ensures consistency between files that may or may not have the "_" prefix
philly_infousa_dt[, pm.zip := str_remove(as.character(pm.zip), "^_") %>%
                    str_pad(5, "left", "0")]
philly_parcels[, pm.zip := str_remove(as.character(pm.zip), "^_") %>%
                 str_pad(5, "left", "0")]

# Direction normalization for merge rules:
# If an InfoUSA address has a direction, allow matching to parcels with either
# the same direction or no direction. Never allow opposite/different direction.
norm_dir <- function(x) {
  y <- str_to_lower(str_squish(as.character(x)))
  y[y %in% c("", "na", "n/a", "null")] <- NA_character_
  y <- case_when(
    y %in% c("n", "north") ~ "n",
    y %in% c("s", "south") ~ "s",
    y %in% c("e", "east")  ~ "e",
    y %in% c("w", "west")  ~ "w",
    TRUE ~ y
  )
  y
}
philly_infousa_dt[, pm.dir_concat := norm_dir(pm.dir_concat)]
philly_parcels[, pm.dir_concat := norm_dir(pm.dir_concat)]

philly_infousa_dt[,GEOID.longitude := as.numeric(ge_longitude_2010)]
philly_infousa_dt[,GEOID.latitude := as.numeric(ge_latitude_2010)]


philly_infousa_dt_address_agg = philly_infousa_dt[!is.na(pm.house) & !is.na(n_sn_ss_c) & n_sn_ss_c!="",list(
  num_obs = .N
), by = .(year, n_sn_ss_c, pm.house, pm.street,pm.zip,
          pm.streetSuf,pm.dir_concat, GEOID.longitude, GEOID.latitude)]

philly_infousa_dt_addys = unique(philly_infousa_dt_address_agg, by = "n_sn_ss_c")

philly_parcels[,PID := str_pad(parcel_number,9, "left",pad = "0") ]
# Keep one row per (address, PID). Do NOT collapse to one PID per address.
# Collapsing by n_sn_ss_c causes sibling PID loss for multi-parcel complexes.
philly_parcel_addys = unique(
  philly_parcels[, .(
    PID, n_sn_ss_c, pm.house, pm.street, pm.zip, pm.streetSuf,
    pm.sufDir, pm.preDir, pm.dir_concat,
    market_value, total_livable_area, building_code_description_new,
    building_code_description, unit
  )],
  by = c("n_sn_ss_c", "PID")
)
philly_parcel_addys[, pm.zip := str_remove(as.character(pm.zip), "^_") %>%
                      str_pad(5, "left", "0")]
philly_parcel_addys[, pm.house := as.character(pm.house)]
assert_unique(philly_parcel_addys, c("n_sn_ss_c", "PID"), "philly_parcel_addys")
# philly_parcel_addys = unique(rbindlist(list(philly_rentals_long_addys %>%
#                                                            select(pm.address:PID, geocode_x, geocode_y),
#                                                 philly_parcel_addys %>%
#                                                   select(pm.address:PID, geocode_x, geocode_y)
#                                                 ), fill=T),
#                                           by = "n_sn_ss_c")

## num st sfx prefix zip ##
# merge the philly_infousa_dt_address_agg data back
num_st_sfx_dir_zip_merge = philly_infousa_dt_addys %>%
  # merge with address data
  # Match on house/street/suffix/zip, then apply directional guardrail.
  merge(philly_parcel_addys[,.( pm.house, pm.street,pm.zip,pm.dir_concat,
                                pm.streetSuf,PID
  )],
  ,by = c("pm.house", "pm.street", "pm.streetSuf", "pm.zip")
  ,all.x= T,
  allow.cartesian = T
  ) %>%
  mutate(
    merge = "num_st_sfx_dir_zip"
  )

setDT(num_st_sfx_dir_zip_merge)
# Directional rule:
# - If InfoUSA has no direction -> keep all candidates.
# - If InfoUSA has direction -> keep parcel direction equal to InfoUSA OR missing.
num_st_sfx_dir_zip_merge[
  ,
  keep_dir := is.na(pm.dir_concat.x) |
    is.na(pm.dir_concat.y) |
    pm.dir_concat.x == pm.dir_concat.y
]
num_st_sfx_dir_zip_merge <- num_st_sfx_dir_zip_merge[keep_dir == TRUE]
num_st_sfx_dir_zip_merge[, c("keep_dir", "pm.dir_concat.x", "pm.dir_concat.y") := NULL]
num_st_sfx_dir_zip_merge[,num_pids := uniqueN(PID,na.rm = T), by = n_sn_ss_c]
## save the merges that are unique ##
num_st_sfx_dir_zip_merge[,matched := num_pids==1]
matched_num_st_sfx_dir_zip = num_st_sfx_dir_zip_merge[matched == T, n_sn_ss_c]


#### summary stats on merge ####
# should really make this a function -- it's ugly code.
unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[,.N, by = num_pids][,per := (N / sum(N)) %>% round(2)][order(N)]
unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[,.N, by = .(num_pids==0)][,per := round(N/ sum(N),2)][order(N)]


(unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[num_pids == 0,.N, by = pm.street][
  ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
    order(-N)][1:20])

(unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[!pm.street %in% philly_parcel_addys$pm.street,.N, by = pm.street][
  ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
    order(-N)][1:20])

(unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[num_pids == 0,.N, by = n_sn_ss_c][
  ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
    order(-N)][1:20])


# View(philly_infousa_dt_address_agg[!pm.street %in% philly_rentals_long$pm.street] )
#
# View(num_st_sfx_dir_zip_merge[num_pids==2] %>% sample_n(100))
# View(num_st_sfx_dir_zip_merge[num_pids==0] %>% sample_n(100))


#### random sampling ####
# View(num_st_sfx_dir_zip_merge[num_pids > 3 ] %>% sample_n(1000))
# View(num_st_sfx_dir_zip_merge[num_pids == 0 &  pm.street %in% num_st_sfx_dir_zip_merge[,sample(pm.street,100) ]  ] %>%
#        #select(-pm.state, -addy_id) %>%
#        arrange(pm.street, pm.house)
# )


#### merge on num st zip ####

num_st_merge = philly_infousa_dt_addys %>%
  filter(
    !n_sn_ss_c %in% matched_num_st_sfx_dir_zip
  ) %>%
  # merge with address data
  # only let merge with parcels that have units aka residential ones
  merge(philly_parcel_addys[,.( pm.house, pm.street,pm.zip,
                                pm.streetSuf, pm.sufDir, pm.preDir, pm.dir_concat, PID
  )],
  ,by = c("pm.house", "pm.street",'pm.zip')
  ,all.x= T,
  allow.cartesian = T
  ) %>%
  mutate(
    merge = "num_st"
  )

setDT(num_st_merge)
num_st_merge[
  ,
  keep_dir := is.na(pm.dir_concat.x) |
    is.na(pm.dir_concat.y) |
    pm.dir_concat.x == pm.dir_concat.y
]
num_st_merge <- num_st_merge[keep_dir == TRUE]
num_st_merge[, c("keep_dir", "pm.dir_concat.x", "pm.dir_concat.y") := NULL]
num_st_merge[,num_pids := uniqueN(PID,na.rm = T), by = n_sn_ss_c]

## keep the merges that are unique ##
num_st_merge[,matched := num_pids==1]
matched_num_st = num_st_merge[matched == T, n_sn_ss_c]

# check that we just kept unique merges...
sum( matched_num_st %in%  matched_num_st_sfx_dir_zip) # should be zero

#### summary stats on merge ####
# should really make this a function -- it's ugly code.
unique(num_st_merge, by = "n_sn_ss_c")[,.N, by = num_pids][,per := (N / sum(N)) %>% round(2)][order(num_pids)]
unique(num_st_merge, by = "n_sn_ss_c")[,.N, by = .(num_pids==0)][,per := round(N/ sum(N),2)][order(num_pids)]
(unique(num_st_merge, by = "n_sn_ss_c")[num_pids == 0,.N, by = pm.street][
  ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
    order(-N)][1:20])

unique(num_st_merge, by = "n_sn_ss_c")[,.N, by = .(num_pids==0)][,per := round(N/ sum(N),2)][order(num_pids)]
(unique(num_st_merge, by = "n_sn_ss_c")[num_pids == 0,.N, by = n_sn_ss_c][
  ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
    order(-N)][1:20])


#### num st sfx merge ####
# Keep fallback matching at unique-address level (not year-level address rows),
# so merge cardinality and "unique merge" logic are driven by address keys only.
num_st_sfx_merge = philly_infousa_dt_addys  %>%
  filter(
    !n_sn_ss_c %in% matched_num_st_sfx_dir_zip &
      !n_sn_ss_c %in% matched_num_st
  ) %>%
  # merge with address data
  # only let merge with parcels that have units aka residential ones
  merge(philly_parcel_addys[,.( pm.house, pm.street,
                                pm.streetSuf, pm.dir_concat, PID
  )],
  ,by = c("pm.house", "pm.street","pm.streetSuf")
  ,all.x= T,
  allow.cartesian = T
  ) %>%
  mutate(
    merge = "num_st_sfx"
  )


setDT(num_st_sfx_merge)
num_st_sfx_merge[
  ,
  keep_dir := is.na(pm.dir_concat.x) |
    is.na(pm.dir_concat.y) |
    pm.dir_concat.x == pm.dir_concat.y
]
num_st_sfx_merge <- num_st_sfx_merge[keep_dir == TRUE]
num_st_sfx_merge[, c("keep_dir", "pm.dir_concat.x", "pm.dir_concat.y") := NULL]
num_st_sfx_merge[,num_pids := uniqueN(PID,na.rm = T), by = n_sn_ss_c]

## keep the merges that are unique ##
num_st_sfx_merge[,matched := num_pids==1]
matched_num_st_sfx = num_st_sfx_merge[matched == T, unique(n_sn_ss_c)]
sum(matched_num_st_sfx %in% matched_num_st)

#### summary stats on merge ####
# should really make this a function -- it's ugly code.
unique(num_st_sfx_merge, by = "n_sn_ss_c")[,.N, by = num_pids][,per := (N / sum(N)) %>% round(2)][order(num_pids)]
unique(num_st_sfx_merge, by = "n_sn_ss_c")[,.N, by = .(num_pids==0)][,per := round(N/ sum(N),2)][order(num_pids)]
unique(num_st_sfx_merge, by = "n_sn_ss_c")[num_pids == 0,.N, by = pm.street][
  ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
    order(-N)][1:20]


#### spatial join ####
# not doing this for now
# set this off otherwise the spatial merge doesnt work for whatever reason
sf_use_s2(FALSE)
# filter to just be parcels that didn't uniquely merge
# spatial_join = num_st_sfx_merge[num_pids != 1]
#
parcel_sf_subset = philly_parcels_sf_m
# reproject parcel_sf_subset to be crs4269
parcel_sf_subset = st_transform(parcel_sf_subset, crs = 4269)

# filter to just be parcels that didn't uniquely merge
spatial_join = num_st_sfx_merge[num_pids != 1]

# get lat long coords from parcel_sf_subset
# make altos into shape file
# note here that altos data is only geocoded to 5 digits or within a meter
# of precission.
# also pretty sure that it's geocoding to the street level in a lot of cases
# so there might be some room to re geocode them and get parcel-level coordinates
spatial_join_sf = spatial_join %>%
  filter(!is.na(GEOID.longitude) & !is.na(GEOID.latitude)) %>%
  st_as_sf(coords = c("GEOID.longitude", "GEOID.latitude")) %>%
  st_set_crs(value = st_crs(parcel_sf_subset))

spatial_join_sf_join = st_join(
  spatial_join_sf,
  parcel_sf_subset %>%
    rename(PID_2 = PID) %>%
    select(PID_2, geometry)# %>% filter(PID %in% fa_expand[, PID])
)

spatial_join_sf_join = spatial_join_sf_join %>%
  as.data.table() %>%
  select(-geometry) %>%
  mutate(merge = "spatial")

spatial_join_sf_join[,num_pids_st := uniqueN(PID_2), by = n_sn_ss_c]

# keep unique matches
spatial_join_sf_join[, matched := num_pids_st == 1 ]
matched_spatial = spatial_join_sf_join[matched == T, n_sn_ss_c]

## summary stats ##
spatial_join_sf_join[,num_pids_st := uniqueN(PID_2,na.rm = T), by = n_sn_ss_c]
unique(spatial_join_sf_join, by = "n_sn_ss_c")[,.N, by = .(num_pids_st==0)][,per := round(N/ sum(N),2)][order(N)]
unique(spatial_join_sf_join, by = "n_sn_ss_c")[,.N, by = num_pids_st][,per := (N / sum(N)) %>% round(2)][order(N)]

(spatial_join_sf_join[num_pids_st == 0,.N, by = pm.street][
  ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
    order(-N)][1:20])

#View(sample_n(spatial_join_sf_join[num_pids_st == 0][,count :=.N, by = n_sn_ss_c] ,1000))


#### Tier 5: Fuzzy house-number match ####
# Target: addresses that failed all 4 tiers AND have >=5 total InfoUSA obs.
# Strategy: match on same street + zip, allow house number within ±10.
# Only keep unique matches (exactly one candidate parcel within the window).

all_matched_so_far <- c(matched_num_st_sfx_dir_zip, matched_num_st,
                        matched_num_st_sfx, matched_spatial)

# Aggregate total observations across years for unmatched addresses
unmatched_for_fuzzy <- philly_infousa_dt_address_agg[
  !n_sn_ss_c %in% all_matched_so_far,
  .(total_obs = sum(num_obs)),
  by = .(n_sn_ss_c, pm.house, pm.street, pm.zip)
]
unmatched_for_fuzzy <- unmatched_for_fuzzy[total_obs >= 5 & !is.na(pm.house) & pm.house != ""]
unmatched_for_fuzzy[, pm.house_num := as.numeric(pm.house)]
unmatched_for_fuzzy <- unmatched_for_fuzzy[!is.na(pm.house_num)]

logf("  [Tier 5] Fuzzy house-number candidates: ", nrow(unmatched_for_fuzzy),
     " unmatched addresses with >=5 obs", log_file = log_file)

if (nrow(unmatched_for_fuzzy) > 0) {
  # Prepare parcel addresses with numeric house numbers
  parcel_fuzzy <- philly_parcel_addys[!is.na(pm.house) & pm.house != "",
                                      .(pm.house, pm.street, pm.zip, PID)]
  parcel_fuzzy[, pm.house_num := as.numeric(pm.house)]
  parcel_fuzzy <- parcel_fuzzy[!is.na(pm.house_num)]

  # Join on street + zip, then filter by house number within ±10
  fuzzy_merge <- merge(
    unmatched_for_fuzzy[, .(n_sn_ss_c, pm.house_num, pm.street, pm.zip)],
    parcel_fuzzy[, .(pm.house_num_parcel = pm.house_num, pm.street, pm.zip, PID)],
    by = c("pm.street", "pm.zip"),
    allow.cartesian = TRUE
  )
  fuzzy_merge[, house_diff := abs(pm.house_num - pm.house_num_parcel)]
  fuzzy_merge <- fuzzy_merge[house_diff > 0 & house_diff <= 10]

  # Keep all candidates; later deterministic disambiguation will pick one PID.
  fuzzy_merge[, num_candidates := uniqueN(PID), by = n_sn_ss_c]
  fuzzy_candidates <- unique(
    fuzzy_merge[, .(PID, n_sn_ss_c, merge = "fuzzy_house_num")]
  )
  fuzzy_unique <- fuzzy_merge[num_candidates == 1]

  matched_fuzzy <- unique(fuzzy_candidates$n_sn_ss_c)

  logf("  [Tier 5] Fuzzy house-number matches: ", length(matched_fuzzy),
       " addresses with candidate parcel(s) (of ", nrow(unmatched_for_fuzzy), " candidates)",
       log_file = log_file)
  if (nrow(fuzzy_merge) > 0) {
    logf("  [Tier 5] Multi-candidate (to resolve via scoring): ",
         fuzzy_merge[num_candidates > 1, uniqueN(n_sn_ss_c)],
         " addresses", log_file = log_file)
  }
} else {
  fuzzy_candidates <- data.table(PID = character(), n_sn_ss_c = character(),
                                 merge = character())
  fuzzy_unique <- data.table(PID = character(), n_sn_ss_c = character(),
                             pm.house_num = numeric(), pm.street = character(),
                             pm.zip = character(), pm.house_num_parcel = numeric(),
                             house_diff = numeric(), num_candidates = integer())
  matched_fuzzy <- character(0)
}

matched_ids = c(
  matched_num_st_sfx_dir_zip,
  matched_num_st,
  matched_num_st_sfx,
  matched_spatial,
  matched_fuzzy
)


#unique(spatial_join_sf_join, by = "n_sn_ss_c_1")[,.N, by = .(n_sn_ss_c_1 %in% (matched))][,per := N / sum(N)][]

#                      )
#matched_new = matched_ids
unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[,.N, by = .(n_sn_ss_c %in% (matched_ids))][,per := N / sum(N)][]
#unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[,.N, by = .(n_sn_ss_c %in% (matched_new))][,per := N / sum(N)][]

# take some random samples of not merged

#### make philly_infousa_dt_address_agg-parcels xwalk ####
# Build candidate PID sets from all tiers; keep the best (lowest-rank tier)
# candidate per (address, PID), then resolve multi-PID addresses deterministically.
xwalk_candidates <- rbindlist(list(
  num_st_sfx_dir_zip_merge[!is.na(PID), .(PID, n_sn_ss_c, merge)],
  num_st_merge[!is.na(PID), .(PID, n_sn_ss_c, merge)],
  num_st_sfx_merge[!is.na(PID), .(PID, n_sn_ss_c, merge)],
  spatial_join_sf_join[!is.na(PID_2), .(PID = PID_2, n_sn_ss_c, merge)],
  fuzzy_candidates[!is.na(PID), .(PID, n_sn_ss_c, merge)]
), fill = TRUE, use.names = TRUE)

xwalk_candidates <- unique(xwalk_candidates)
xwalk_candidates[, merge_rank := fcase(
  merge == "num_st_sfx_dir_zip", 1L,
  merge == "num_st", 2L,
  merge == "num_st_sfx", 3L,
  merge == "spatial", 4L,
  merge == "fuzzy_house_num", 5L,
  default = 99L
)]
setorder(xwalk_candidates, n_sn_ss_c, PID, merge_rank)
xwalk_candidates <- xwalk_candidates[, .SD[1], by = .(n_sn_ss_c, PID)]
assert_unique(xwalk_candidates, c("n_sn_ss_c", "PID"), "xwalk_candidates")

# Address-level household scale from InfoUSA, used for candidate plausibility.
addr_hh_stats <- philly_infousa_dt_address_agg[
  ,
  .(
    hh_max_year = max(num_obs, na.rm = TRUE),
    hh_mean_year = mean(num_obs, na.rm = TRUE),
    hh_total = sum(num_obs, na.rm = TRUE),
    hh_years = .N
  ),
  by = n_sn_ss_c
]

# PID-level parcel features for disambiguation among sibling candidates.
parcel_features <- unique(
  philly_parcel_addys[
    ,
    .(
      PID,
      market_value = suppressWarnings(as.numeric(market_value)),
      total_livable_area = suppressWarnings(as.numeric(total_livable_area)),
      building_code_description_new,
      building_code_description,
      unit
    )
  ],
  by = "PID"
)

cand_scored <- merge(
  xwalk_candidates,
  addr_hh_stats,
  by = "n_sn_ss_c",
  all.x = TRUE
)
cand_scored <- merge(
  cand_scored,
  parcel_features,
  by = "PID",
  all.x = TRUE
)

cand_scored[, bldg_desc := str_to_lower(str_squish(
  fifelse(
    !is.na(building_code_description_new) & building_code_description_new != "",
    as.character(building_code_description_new),
    as.character(building_code_description)
  )
))]
cand_scored[is.na(bldg_desc), bldg_desc := ""]
cand_scored[, has_unit_token := !is.na(unit) & str_squish(as.character(unit)) != ""]

residential_re <- "(apart|apts|resident|dwelling|duplex|triplex|multi\\s*family|condo|townhouse|rowhome|flat)"
nonres_re <- "(retail|office|industrial|warehouse|parking|garage|vacant|school|hospital|hotel|commercial|store)"

cand_scored[, is_residential_like := str_detect(bldg_desc, residential_re)]
cand_scored[, is_nonres_like := str_detect(bldg_desc, nonres_re)]

cand_scored[is.na(total_livable_area), total_livable_area := 0]
cand_scored[is.na(market_value), market_value := 0]
cand_scored[is.na(hh_max_year), hh_max_year := 0]

cand_scored[, large_fit := hh_max_year >= 30 &
              is_residential_like &
              !is_nonres_like &
              !has_unit_token &
              total_livable_area >= pmax(10000, hh_max_year * 220)]

cand_scored[, score :=
              120 * as.numeric(is_residential_like) -
              140 * as.numeric(is_nonres_like) -
               80 * as.numeric(hh_max_year >= 30 & has_unit_token) +
               40 * as.numeric(large_fit) +
               25 * log1p(pmax(total_livable_area, 0)) +
                8 * log1p(pmax(market_value, 0)) -
               10 * as.numeric(merge_rank)]

cand_scored[, n_candidates := .N, by = n_sn_ss_c]

resolved <- cand_scored[
  ,
  {
    ord_default <- order(-score, merge_rank, -market_value, -total_livable_area, PID)
    top_scores <- sort(score, decreasing = TRUE)
    score_margin <- if (length(top_scores) >= 2) top_scores[1] - top_scores[2] else NA_real_

    idx_large <- which(large_fit)
    chosen_idx <- NA_integer_
    resolution_rule <- NA_character_

    if (.N == 1L) {
      chosen_idx <- 1L
      resolution_rule <- "single_candidate"
    } else if (length(idx_large) == 1L) {
      chosen_idx <- idx_large[1]
      resolution_rule <- "single_large_fit"
    } else if (length(idx_large) > 1L) {
      sub_dt <- .SD[idx_large]
      sub_ord <- order(-sub_dt$market_value, -sub_dt$total_livable_area, sub_dt$merge_rank, sub_dt$PID)
      chosen_idx <- idx_large[sub_ord[1]]
      resolution_rule <- "multi_large_fit_weighted_mv"
    } else {
      chosen_idx <- ord_default[1]
      resolution_rule <- "score_weighted_mv"
    }

    .(
      PID = PID[chosen_idx],
      merge = merge[chosen_idx],
      resolution_rule = resolution_rule,
      resolution_score_margin = score_margin
    )
  },
  by = n_sn_ss_c
]
assert_unique(resolved, "n_sn_ss_c", "resolved_infousa_xwalk")

# QA export: candidate scoring details for multi-PID addresses.
qa_resolution <- merge(
  cand_scored,
  resolved[, .(n_sn_ss_c, PID_selected = PID, resolution_rule, resolution_score_margin)],
  by = "n_sn_ss_c",
  all.x = TRUE
)
qa_resolution[, selected := PID == PID_selected]

qa_resolution_path <- p_out(cfg, "qa", "infousa_multi_pid_resolution.csv")
dir.create(dirname(qa_resolution_path), recursive = TRUE, showWarnings = FALSE)
fwrite(
  qa_resolution[
    ,
    .(
      n_sn_ss_c, PID, selected, merge, merge_rank, n_candidates,
      resolution_rule, resolution_score_margin, hh_max_year, hh_mean_year,
      total_livable_area, market_value, bldg_desc, has_unit_token,
      is_residential_like, is_nonres_like, large_fit, score
    )
  ],
  qa_resolution_path
)
logf("  Wrote multi-PID resolution QA: ", qa_resolution_path, log_file = log_file)
logf("  Multi-PID addresses resolved via scoring: ",
     qa_resolution[n_candidates > 1, uniqueN(n_sn_ss_c)],
     log_file = log_file)

all_addrs <- data.table(n_sn_ss_c = unique(philly_infousa_dt_address_agg$n_sn_ss_c))
xwalk <- merge(
  all_addrs,
  resolved[, .(n_sn_ss_c, PID, merge, resolution_rule)],
  by = "n_sn_ss_c",
  all.x = TRUE
)
xwalk[is.na(PID), `:=`(merge = "not merged", resolution_rule = "unmatched")]
xwalk[, unique := TRUE]
xwalk[, num_merges := 1L]
xwalk[, num_parcels_matched := as.integer(!is.na(PID))]
xwalk[, num_addys_matched := uniqueN(n_sn_ss_c), by = PID]
xwalk[is.na(PID), num_addys_matched := 0L]
assert_unique(xwalk, "n_sn_ss_c", "xwalk final")

unique(xwalk, by = "n_sn_ss_c")[, .N, by = .(merge)][, per := round(N / sum(N), 4)]
unique(xwalk, by = "n_sn_ss_c")[, .N, by = .(num_parcels_matched)][
  , per := round(N / sum(N), 4)][order(num_parcels_matched)]

xwalk_listing = merge(
  xwalk,
  unique(philly_infousa_dt[, .(n_sn_ss_c, obs_id)]),
  by = "n_sn_ss_c",
  allow.cartesian = TRUE
)

#### Merge Statistics Summary ####
logf("Merge statistics...", log_file = log_file)

# Summary by merge type
merge_summary <- xwalk[, .N, by = merge][, per := round(N / sum(N), 2)][order(-N)]
logf("  By merge type:", log_file = log_file)
for (i in seq_len(nrow(merge_summary))) {
  logf("    ", merge_summary$merge[i], ": ", merge_summary$N[i], " (", merge_summary$per[i] * 100, "%)", log_file = log_file)
}

# Match rate
n_matched <- xwalk[!is.na(PID) & PID != "", uniqueN(n_sn_ss_c)]
n_total <- xwalk[, uniqueN(n_sn_ss_c)]
match_rate <- round(n_matched / n_total * 100, 1)
logf("  Overall match rate: ", n_matched, " / ", n_total, " addresses (", match_rate, "%)", log_file = log_file)

# Parcels per address
parcels_dist <- unique(xwalk, by = "n_sn_ss_c")[, .N, by = num_parcels_matched][order(num_parcels_matched)]
logf("  Parcels per address distribution:", log_file = log_file)
for (i in seq_len(min(5, nrow(parcels_dist)))) {
  logf("    ", parcels_dist$num_parcels_matched[i], " parcels: ", parcels_dist$N[i], " addresses", log_file = log_file)
}

#### export ####
logf("Writing outputs...", log_file = log_file)

out_xwalk <- p_product(cfg, "infousa_address_xwalk")
out_agg <- p_product(cfg, "infousa_address_agg")

fwrite(xwalk, out_xwalk)
logf("  Wrote infousa_address_xwalk: ", nrow(xwalk), " rows to ", out_xwalk, log_file = log_file)

fwrite(philly_infousa_dt_address_agg, out_agg)
logf("  Wrote infousa_address_agg: ", nrow(philly_infousa_dt_address_agg), " rows to ", out_agg, log_file = log_file)

# Optional: xwalk_listing (per-observation crosswalk)
out_xwalk_listing <- p_proc(cfg, "xwalks/philly_infousa_dt_address_agg_xwalk_listing.csv")
fwrite(xwalk_listing, out_xwalk_listing)
logf("  Wrote xwalk_listing: ", nrow(xwalk_listing), " rows to ", out_xwalk_listing, log_file = log_file)

logf("=== Finished merge-infousa-parcels.R ===", log_file = log_file)
