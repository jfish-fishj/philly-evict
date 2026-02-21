## ============================================================
## merge-evictions-parcels.R
## ============================================================
## Purpose: Merge eviction filings with parcels via tiered address matching
##          Creates address crosswalk for linking evictions to parcels
##
## Inputs:
##   - cfg$products$licenses_clean (clean/licenses_clean.csv)
##   - cfg$products$evictions_clean (clean/evictions_clean.csv)
##   - cfg$products$parcels_clean (clean/parcels_clean.csv)
##   - cfg$inputs$philly_parcels_sf (shapefiles/philly_parcels_sf/DOR_Parcel.shp)
##
## Outputs:
##   - cfg$products$evict_address_xwalk (xwalks/philly_evict_address_agg_xwalk.csv)
##   - cfg$products$evict_address_agg (xwalks/philly_evict_address_agg.csv)
##   - cfg$products$evict_address_xwalk_case (xwalks/philly_evict_address_agg_xwalk_case.csv)
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
source("R/helper-functions.R")
source("r/lib/address_utils.R")
source("r/lib/qa_utils.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "merge-evictions-parcels.log")
qa_dir <- p_out(cfg, "qa", "merge-evict-rentals")
if (!dir.exists(qa_dir)) dir.create(qa_dir, recursive = TRUE)

logf("=== Starting merge-evictions-parcels.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ---- Load input data ----
logf("Loading input data...", log_file = log_file)

philly_lic <- fread(p_product(cfg, "licenses_clean"))
philly_evict <- fread(p_product(cfg, "evictions_clean"))
philly_parcels <- fread(p_product(cfg, "parcels_clean"))
philly_parcels_sf <- read_sf(p_input(cfg, "philly_parcels_sf"))

logf("  Licenses: ", nrow(philly_lic), " rows", log_file = log_file)
logf("  Evictions: ", nrow(philly_evict), " rows", log_file = log_file)
logf("  Parcels: ", nrow(philly_parcels), " rows", log_file = log_file)


philly_rentals = philly_lic[licensetype == "Rental"]
philly_rentals[,start_year := as.numeric(substr(initialissuedate,1,4))]
philly_rentals[,end_year := as.numeric(substr(expirationdate,1,4))]
philly_rentals[,num_years := end_year - start_year]
philly_rentals[,id := .I]
# Normalize pm.zip for merging: strip "_" prefix, pad to 5 digits
philly_rentals[, pm.zip := coalesce(
  str_remove(as.character(pm.zip), "^_"),
  str_sub(zip, 1, 5)
) %>% str_pad(5, "left", pad = "0")]

philly_rentals = philly_rentals %>%
  mutate(
    geocode_y = lng,
    geocode_x = lat
  )


philly_rentals_long = uncount(philly_rentals[num_years > 0], num_years) %>%
  group_by(id) %>%
  mutate(
    year = start_year + row_number() - 1
  ) %>%
  ungroup() %>%
  as.data.table()

philly_rentals_long[,opa_account_num := str_pad(opa_account_num,9, "left",pad = "0")]
philly_rentals_long[,PID := opa_account_num]
philly_rentals_long = philly_rentals_long[year %in% 2016:2024 & n_sn_ss_c != ""]

# now add in the

# clean some names
philly_evict[,clean_defendant_name := clean_name(defendant)]
# flag commercial
philly_evict[,commercial_alt := str_detect(clean_defendant_name, business_regex)]
# Normalize pm.zip: strip "_" prefix, pad to 5 digits
philly_evict[, pm.zip := str_remove(as.character(pm.zip), "^_") %>%
               str_pad(5, "left", pad = "0")]

philly_evict[,dup := .N, by = .(n_sn_ss_c, plaintiff,defendant,d_filing)][,dup := dup > 1]

# plot evictions by year
ev_year_aggs = philly_evict[commercial == "f" &
                              commercial_alt == F &
                              dup == F &
                              year >= 2000 &
                              total_rent <= 5e4 &
                              !is.na(n_sn_ss_c), .N, by = year][order(year)]


philly_evict_address_agg = philly_evict[commercial == "f" &
                                          commercial_alt == F &
                                          dup == F &
                                          year >= 2000 &
                                          total_rent <= 5e4 &
                                          !is.na(n_sn_ss_c), list(num_evict = .N), by = .(
                                            year,
                                            n_sn_ss_c,
                                            pm.house,
                                            pm.street,
                                            pm.zip,
                                            pm.streetSuf,
                                            pm.sufDir,
                                            pm.preDir,
                                            longitude,
                                            latitude
                                          )]

philly_rentals_long_addys = unique(philly_rentals_long, by = "n_sn_ss_c")
philly_evict_addys = unique(philly_evict_address_agg, by = "n_sn_ss_c")
philly_evict_addys[,pm.sufDir := replace_na(as.character(pm.sufDir), "")]

philly_parcels[,PID := str_pad(parcel_number,9, "left",pad = "0") ]
philly_parcel_addys = unique(philly_parcels, by = "n_sn_ss_c")
# Normalize pm.zip: strip "_" prefix, pad to 5 digits
philly_parcel_addys[, pm.zip := str_remove(as.character(pm.zip), "^_") %>%
                      str_pad(5, "left", pad = "0")]
# philly_parcel_rentals_long_addys = unique(rbindlist(list(philly_rentals_long_addys %>%
#                                                            select(pm.address:PID, geocode_x, geocode_y),
#                                                 philly_parcel_addys %>%
#                                                   select(pm.address:PID, geocode_x, geocode_y)
#                                                 ), fill=T),
#                                           by = "n_sn_ss_c")

## num st sfx prefix zip ##
# merge the philly_evict_address_agg data back
num_st_sfx_dir_zip_merge = philly_evict_addys %>%
  # merge with address data
  # only let merge with parcels that have units aka residential ones
  merge(philly_parcel_addys[,.( pm.house, pm.street,pm.zip,
                                    pm.streetSuf, pm.sufDir, pm.preDir,PID
                                   )],
    ,by = c("pm.house", "pm.street", "pm.streetSuf", "pm.sufDir", "pm.preDir", "pm.zip")
    ,all.x= T,
    allow.cartesian = T
  ) %>%
  mutate(
    merge = "num_st_sfx_dir_zip"
  )

setDT(num_st_sfx_dir_zip_merge)
num_st_sfx_dir_zip_merge[,num_pids := uniqueN(PID,na.rm = T), by = n_sn_ss_c]
## save the merges that are unique ##
num_st_sfx_dir_zip_merge[,matched := num_pids==1]
matched_num_st_sfx_dir_zip = num_st_sfx_dir_zip_merge[matched == T, n_sn_ss_c]


#### Tier 1 summary stats ####
tier1_stats <- merge_summary_stats(num_st_sfx_dir_zip_merge, label = "tier_1_num_st_sfx_dir_zip")
log_merge_stats(tier1_stats, "Tier 1: num_st_sfx_dir_zip", log_file = log_file, logf_fn = logf)
write_merge_summary(tier1_stats, qa_dir, "tier_1")


#### merge on num st zip ####

num_st_merge = philly_evict_addys %>%
  filter(
      !n_sn_ss_c %in% matched_num_st_sfx_dir_zip
  ) %>%
  # merge with address data
  # only let merge with parcels that have units aka residential ones
  merge(philly_parcel_addys[,.( pm.house, pm.street,pm.zip,
                                      pm.streetSuf, pm.sufDir, pm.preDir,PID
  )],
  ,by = c("pm.house", "pm.street",'pm.zip')
  ,all.x= T,
  allow.cartesian = T
  ) %>%
  mutate(
    merge = "num_st"
  )

setDT(num_st_merge)
num_st_merge[,num_pids := uniqueN(PID,na.rm = T), by = n_sn_ss_c]

## keep the merges that are unique ##
num_st_merge[,matched := num_pids==1]
matched_num_st = num_st_merge[matched == T, n_sn_ss_c]

# check that we just kept unique merges...
sum( matched_num_st %in%  matched_num_st_sfx_dir_zip) # should be zero

#### Tier 2 summary stats ####
tier2_stats <- merge_summary_stats(num_st_merge, label = "tier_2_num_st")
log_merge_stats(tier2_stats, "Tier 2: num_st (zip)", log_file = log_file, logf_fn = logf)
write_merge_summary(tier2_stats, qa_dir, "tier_2")


#### num st sfx merge ####
num_st_sfx_merge = philly_evict_address_agg  %>%
  filter(
    !n_sn_ss_c %in% matched_num_st_sfx_dir_zip &
      !n_sn_ss_c %in% matched_num_st
  ) %>%
  # merge with address data
  # only let merge with parcels that have units aka residential ones
  merge(philly_parcel_addys[,.( pm.house, pm.street,
                                      pm.streetSuf,PID
  )],
  ,by = c("pm.house", "pm.street","pm.streetSuf")
  ,all.x= T,
  allow.cartesian = T
  ) %>%
  mutate(
    merge = "num_st_sfx"
  )


setDT(num_st_sfx_merge)
num_st_sfx_merge[,num_pids := uniqueN(PID,na.rm = T), by = n_sn_ss_c]

## keep the merges that are unique ##
num_st_sfx_merge[,matched := num_pids==1]
matched_num_st_sfx = num_st_sfx_merge[matched == T, unique(n_sn_ss_c)]
sum(matched_num_st_sfx %in% matched_num_st)

#### Tier 3 summary stats ####
tier3_stats <- merge_summary_stats(num_st_sfx_merge, label = "tier_3_num_st_sfx")
log_merge_stats(tier3_stats, "Tier 3: num_st_sfx", log_file = log_file, logf_fn = logf)
write_merge_summary(tier3_stats, qa_dir, "tier_3")

#### spatial join ####
# not doing this for now
# set this off otherwise the spatial merge doesnt work for whatever reason
sf_use_s2(FALSE)
# filter to just be parcels that didn't uniquely merge
# spatial_join = num_st_sfx_merge[num_pids != 1]
#
philly_parcels_sf_m = philly_parcels_sf %>% select(-matches("PID")) %>%
  merge(philly_parcels[,.(pin, PID)], by.x = "PIN", by.y = "pin")


parcel_sf_subset = philly_parcels_sf_m %>%
  filter(PID %in% philly_rentals_long_addys$PID |
           PID %in% philly_parcel_addys[category_code_description %in% c("MIXED USE",
                                                                         "MULTI FAMILY",
                                                                         "SINGLE FAMILY",
                                                                         "APARTMENTS  > 4 UNITS"),PID])
# make parcel_sf_subset into crs 4269
parcel_sf_subset = parcel_sf_subset %>%
  st_transform(crs = 4269)
# filter to just be parcels that didn't uniquely merge
spatial_join = num_st_sfx_merge[num_pids != 1]

# make altos into shape file
# note here that altos data is only geocoded to 5 digits or within a meter
# of precission.
# also pretty sure that it's geocoding to the street level in a lot of cases
# so there might be some room to re geocode them and get parcel-level coordinates
spatial_join_sf = spatial_join %>%
  filter(!is.na(longitude) & !is.na(latitude)) %>%
  st_as_sf(coords = c("longitude", "latitude")) %>%
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

## Tier 4 (spatial) summary stats ##
# Rename for consistency with other tiers
spatial_join_sf_join[, num_pids := num_pids_st]
tier4_stats <- merge_summary_stats(spatial_join_sf_join, label = "tier_4_spatial")
log_merge_stats(tier4_stats, "Tier 4: spatial", log_file = log_file, logf_fn = logf)
write_merge_summary(tier4_stats, qa_dir, "tier_4")

#### Base xwalks from existing tiers ####
xwalk_unique_base = bind_rows(list(
  num_st_sfx_dir_zip_merge[num_pids == 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  num_st_merge[num_pids == 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  num_st_sfx_merge[num_pids == 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  spatial_join_sf_join[num_pids_st == 1 , .(PID_2,n_sn_ss_c, merge)]  %>%
    distinct()%>%
    rename(PID = PID_2)
)) %>%
  mutate(unique = T) %>%
  as.data.table()

xwalk_non_unique_base = bind_rows(list(
  num_st_sfx_dir_zip_merge[num_pids > 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  num_st_merge[num_pids > 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  num_st_sfx_merge[num_pids > 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  spatial_join_sf_join[num_pids_st > 1 , .(PID_2,n_sn_ss_c, merge)]  %>%
    distinct()%>%
    rename(PID = PID_2)
)) %>%
  mutate(unique = F) %>%
  filter(!n_sn_ss_c %in% xwalk_unique_base$n_sn_ss_c) %>%
  filter(!is.na(PID)) %>%
  distinct(PID, n_sn_ss_c,.keep_all = T) %>%
  as.data.table()

#### Rule-based recovery for unmatched "other" addresses ####
logf("Running rule-based recovery on unmatched_reason == 'other' addresses...", log_file = log_file)

normalize_key <- function(x) {
  x <- tolower(as.character(x))
  x[is.na(x)] <- ""
  str_squish(x)
}

parse_key_fields <- function(dt, key_col = "n_sn_ss_c") {
  out <- copy(as.data.table(dt))
  suffix_terms <- c(
    "st","street","ave","avenue","blvd","boulevard","rd","road","ln","lane","dr","drive","ct","court",
    "pl","place","ter","terrace","way","cir","circle","pkwy","parkway","hwy","highway",
    "gdn","garden","gardens","sq","square","plz","plaza","mall"
  )
  dir_terms <- c("n","s","e","w","ne","nw","se","sw")

  out[, key_norm := normalize_key(get(key_col))]
  out[, house_tok := stringr::word(key_norm, 1L)]
  out[, street_raw := str_squish(str_replace(key_norm, "^\\S+\\s*", ""))]
  out[, last_tok := stringr::word(street_raw, -1L)]
  out[, street_no_suffix := fifelse(
    last_tok %chin% suffix_terms,
    str_squish(str_replace(street_raw, "\\s+\\S+$", "")),
    street_raw
  )]
  out[, lead_tok := stringr::word(street_no_suffix, 1L)]
  out[, street_nopredir_suffix := fifelse(
    lead_tok %chin% dir_terms,
    str_squish(str_replace(street_no_suffix, "^\\S+\\s*", "")),
    street_no_suffix
  )]
  out[, house_num := suppressWarnings(as.integer(str_extract(house_tok, "^[0-9]+")))]
  out
}

summarize_candidates <- function(cand, key_col = "n_sn_ss_c") {
  cand <- unique(cand[!is.na(PID), .(key = get(key_col), PID)])
  if (nrow(cand) == 0L) {
    return(list(unique_keys = character(0), ambig_keys = character(0), n_pid = data.table()))
  }
  n_pid <- cand[, .(n_pid = uniqueN(PID)), by = key]
  list(
    unique_keys = n_pid[n_pid == 1L, key],
    ambig_keys = n_pid[n_pid > 1L, key],
    n_pid = n_pid
  )
}

# Stage current unmatched keys before rule recovery.
unmatched_pre_rule <- philly_evict_address_agg[
  !n_sn_ss_c %in% c(xwalk_unique_base$n_sn_ss_c, xwalk_non_unique_base$n_sn_ss_c),
  unique(n_sn_ss_c)
]

src_unique_rule <- unique(philly_evict_addys[n_sn_ss_c %in% unmatched_pre_rule], by = "n_sn_ss_c")
parcel_unique_rule <- unique(philly_parcel_addys, by = "n_sn_ss_c")

diag_rule <- diagnose_unmatched_addresses(
  src_addys = src_unique_rule,
  parcel_addys = parcel_unique_rule,
  addr_key = "n_sn_ss_c",
  street_col = "pm.street",
  house_col = "pm.house"
)

rule_pool_keys <- diag_rule$flagged[unmatched_reason == "other" & !is.na(n_sn_ss_c), unique(n_sn_ss_c)]
src_rule <- parse_key_fields(src_unique_rule[n_sn_ss_c %in% rule_pool_keys, .(n_sn_ss_c)])
parcel_rule <- parse_key_fields(unique(philly_parcel_addys[, .(n_sn_ss_c, PID)], by = c("n_sn_ss_c", "PID")))
parcel_rule <- parcel_rule[!is.na(PID) & nzchar(as.character(PID))]

rule_assign <- src_rule[, .(
  n_sn_ss_c,
  recovery_rule = NA_character_,
  recovery_status = "unrecovered",
  candidate_n_pid = as.integer(NA)
)]
remaining_keys <- copy(rule_assign$n_sn_ss_c)
rule_step_log <- data.table(
  rule = character(),
  n_remaining_before = integer(),
  recovered_unique_rows = integer(),
  recovered_unique_cases = integer(),
  ambiguous_rows = integer()
)
rule_matches_unique <- data.table(PID = character(), n_sn_ss_c = character(), merge = character(), unique = logical())

apply_rule_from_candidates <- function(rule_name, cand_dt) {
  n_before <- length(remaining_keys)
  if (nrow(cand_dt) == 0L) {
    rule_step_log <<- rbind(rule_step_log, data.table(
      rule = rule_name, n_remaining_before = n_before,
      recovered_unique_rows = 0L, recovered_unique_cases = 0L, ambiguous_rows = 0L
    ))
    return(invisible(NULL))
  }
  s <- summarize_candidates(cand_dt, key_col = "n_sn_ss_c")
  unique_keys <- intersect(s$unique_keys, remaining_keys)
  ambig_keys <- intersect(s$ambig_keys, remaining_keys)

  if (length(unique_keys) > 0L) {
    picked <- unique(cand_dt[n_sn_ss_c %in% unique_keys, .(n_sn_ss_c, PID)])[,
      .SD[1], by = n_sn_ss_c
    ]
    picked[, `:=`(merge = rule_name, unique = TRUE)]
    rule_matches_unique <<- rbind(rule_matches_unique, picked[, .(PID, n_sn_ss_c, merge, unique)], fill = TRUE)
    rule_assign[n_sn_ss_c %in% unique_keys, `:=`(recovery_rule = rule_name, recovery_status = "recovered_unique", candidate_n_pid = 1L)]
    remaining_keys <<- setdiff(remaining_keys, unique_keys)
  }

  if (length(ambig_keys) > 0L) {
    np <- s$n_pid[key %in% ambig_keys, .(n_sn_ss_c = key, n_pid)]
    rule_assign[np, on = "n_sn_ss_c", candidate_n_pid := pmax(candidate_n_pid, i.n_pid, na.rm = TRUE)]
  }

  rule_step_log <<- rbind(rule_step_log, data.table(
    rule = rule_name,
    n_remaining_before = n_before,
    recovered_unique_rows = length(unique_keys),
    recovered_unique_cases = length(unique_keys),
    ambiguous_rows = length(ambig_keys)
  ))
  invisible(NULL)
}

# Rule 1: same house number + fuzzy street typo (edit distance <= 2), unique PID only.
src_r1 <- src_rule[n_sn_ss_c %in% remaining_keys & !is.na(house_num) & nzchar(street_raw)]
cand_r1_list <- vector("list", nrow(src_r1))
if (nrow(src_r1) > 0L) {
  for (ii in seq_len(nrow(src_r1))) {
    s <- src_r1[ii]
    pc <- parcel_rule[house_num == s$house_num & !is.na(street_raw)]
    if (nrow(pc) == 0L) next
    pc <- pc[abs(nchar(street_raw) - nchar(s$street_raw)) <= 2L]
    if (nrow(pc) == 0L) next
    d <- as.integer(adist(s$street_raw, pc$street_raw, ignore.case = TRUE))
    pc <- pc[d <= 2L]
    if (nrow(pc) == 0L) next
    cand_r1_list[[ii]] <- pc[, .(n_sn_ss_c = s$n_sn_ss_c, PID)]
  }
}
cand_r1 <- rbindlist(cand_r1_list, fill = TRUE)
apply_rule_from_candidates("rule_same_house_fuzzy_street_le2", cand_r1)

# Rule 2: same normalized street, nearest house <= 2, unique PID only.
src_r2 <- src_rule[n_sn_ss_c %in% remaining_keys & !is.na(house_num) & nzchar(street_nopredir_suffix), .(n_sn_ss_c, house_num, street_nopredir_suffix)]
par_r2 <- parcel_rule[!is.na(house_num) & nzchar(street_nopredir_suffix), .(PID, house_num_p = house_num, street_nopredir_suffix)]
cand_r2 <- merge(src_r2, par_r2, by = "street_nopredir_suffix", all = FALSE, allow.cartesian = TRUE)
if (nrow(cand_r2) > 0L) {
  cand_r2[, house_diff := abs(house_num - house_num_p)]
  cand_r2 <- cand_r2[, .SD[house_diff == min(house_diff)], by = n_sn_ss_c]
  cand_r2 <- cand_r2[house_diff <= 2L, .(n_sn_ss_c, PID)]
}
apply_rule_from_candidates("rule_same_street_nearest_house_le2", cand_r2)

# Rule 3: same normalized street, nearest house <= 10 (and >2), unique PID only.
src_r3 <- src_rule[n_sn_ss_c %in% remaining_keys & !is.na(house_num) & nzchar(street_nopredir_suffix), .(n_sn_ss_c, house_num, street_nopredir_suffix)]
cand_r3 <- merge(src_r3, par_r2, by = "street_nopredir_suffix", all = FALSE, allow.cartesian = TRUE)
if (nrow(cand_r3) > 0L) {
  cand_r3[, house_diff := abs(house_num - house_num_p)]
  cand_r3 <- cand_r3[, .SD[house_diff == min(house_diff)], by = n_sn_ss_c]
  cand_r3 <- cand_r3[house_diff > 2L & house_diff <= 10L, .(n_sn_ss_c, PID)]
}
apply_rule_from_candidates("rule_same_street_nearest_house_le10", cand_r3)

rule_matches_unique <- unique(rule_matches_unique)
rule_matches_unique <- rule_matches_unique[!n_sn_ss_c %in% xwalk_unique_base$n_sn_ss_c]

rule_step_summary <- merge(
  rule_step_log,
  rule_assign[recovery_status == "recovered_unique", .(final_assigned_rows = .N), by = recovery_rule][, .(rule = recovery_rule, final_assigned_rows)],
  by = "rule",
  all.x = TRUE
)

fwrite(rule_step_summary, file.path(qa_dir, "rule_recovery_step_summary.csv"))
fwrite(rule_assign, file.path(qa_dir, "rule_recovery_assignments.csv"))
fwrite(rule_matches_unique, file.path(qa_dir, "rule_recovery_unique_matches.csv"))
logf("  Rule recovery unique matches: ", nrow(rule_matches_unique), log_file = log_file)
if (nrow(rule_step_summary) > 0L) {
  logf("  Rule recovery step summary:", log_file = log_file)
  for (i in seq_len(nrow(rule_step_summary))) {
    rr <- rule_step_summary[i]
    logf("    ", rr$rule, ": unique=", rr$recovered_unique_rows, ", ambiguous=", rr$ambiguous_rows, log_file = log_file)
  }
}


#### make philly_evict_address_agg-parcels xwalk ####
# first start with parcels that merged uniquely
spatial_join_sf_join = spatial_join_sf_join %>%
  mutate(n_sn_ss_c = n_sn_ss_c,
         merge = "spatial")
xwalk_unique = bind_rows(list(
  xwalk_unique_base[, .(PID, n_sn_ss_c, merge, unique)],
  rule_matches_unique[, .(PID, n_sn_ss_c, merge, unique)]
)) %>%
  distinct(PID, n_sn_ss_c, .keep_all = TRUE) %>%
  as.data.table()

# next append parcels that did not merge uniquely
xwalk_non_unique = xwalk_non_unique_base %>%
  filter(!n_sn_ss_c %in% xwalk_unique$n_sn_ss_c) %>%
  filter(!is.na(PID)) %>%
  distinct(PID, n_sn_ss_c,.keep_all = T) %>%
  as.data.table()


xwalk = bind_rows(list(
  xwalk_unique,
  xwalk_non_unique,
  tibble(n_sn_ss_c = philly_evict_address_agg[(!n_sn_ss_c %in% xwalk_non_unique$n_sn_ss_c &
                                     !n_sn_ss_c %in% xwalk_unique$n_sn_ss_c), n_sn_ss_c],
         merge = "not merged", PID = NA) %>% distinct()
) )

xwalk[,num_merges := uniqueN(merge), by = .(n_sn_ss_c, unique)]
xwalk[,num_parcels_matched := uniqueN(PID,na.rm = T), by = n_sn_ss_c]
xwalk[,num_addys_matched := uniqueN(n_sn_ss_c,na.rm = T), by = PID]

xwalk_case = merge(
  xwalk,
  philly_evict[n_sn_ss_c != "",.(n_sn_ss_c, id)],
  by = "n_sn_ss_c"
)

#### Unmatched Address Diagnostics ####
logf("Running unmatched address diagnostics...", log_file = log_file)

# Get unique unmatched source addresses
src_unique <- unique(philly_evict_addys, by = "n_sn_ss_c")
parc_unique <- unique(philly_parcel_addys, by = "n_sn_ss_c")

# Diagnose unmatched
diag <- diagnose_unmatched_addresses(
  src_addys = src_unique,
  parcel_addys = parc_unique,
  addr_key = "n_sn_ss_c",
  street_col = "pm.street",
  house_col = "pm.house"
)

# Log reason summary
if (nrow(diag$reason_counts) > 0) {
  logf("  Unmatched reason breakdown:", log_file = log_file)
  for (i in seq_len(nrow(diag$reason_counts))) {
    row <- diag$reason_counts[i]
    logf("    ", row$unmatched_reason, ": ", row$N, " (", round(row$pct * 100, 1), "%)", log_file = log_file)
  }
}

# Write diagnostics
write_unmatched_diagnostics(diag, qa_dir, "unmatched")
logf("  Wrote unmatched diagnostics to: ", qa_dir, log_file = log_file)

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

out_xwalk <- p_product(cfg, "evict_address_xwalk")
out_agg <- p_product(cfg, "evict_address_agg")
out_xwalk_case <- p_product(cfg, "evict_address_xwalk_case")

fwrite(xwalk, out_xwalk)
logf("  Wrote evict_address_xwalk: ", nrow(xwalk), " rows to ", out_xwalk, log_file = log_file)

fwrite(philly_evict_address_agg, out_agg)
logf("  Wrote evict_address_agg: ", nrow(philly_evict_address_agg), " rows to ", out_agg, log_file = log_file)

fwrite(xwalk_case, out_xwalk_case)
logf("  Wrote evict_address_xwalk_case: ", nrow(xwalk_case), " rows to ", out_xwalk_case, log_file = log_file)

logf("=== Finished merge-evictions-parcels.R ===", log_file = log_file)
