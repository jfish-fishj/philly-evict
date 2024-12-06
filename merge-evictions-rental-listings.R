library(data.table)
library(tidyverse)
library(sf)
library(tidycensus)

philly_lic = fread("/Users/joefish/Desktop/data/philly-evict/business_licenses_clean.csv")
philly_evict = fread("/Users/joefish/Desktop/data/philly-evict/evict_address_cleaned.csv")
philly_parcels = fread("/Users/joefish/Desktop/data/philly-evict/parcel_address_cleaned.csv")
#rgdal::ogrInfo(system.file("/Users/joefish/Desktop/data/philly-evict/opa_properties_public.gdb", package="sf"))

philly_rentals = philly_lic[licensetype == "Rental"]
philly_rentals[,start_year := as.numeric(substr(initialissuedate,1,4))]
philly_rentals[,end_year := as.numeric(substr(expirationdate,1,4))]
philly_rentals[,num_years := end_year - start_year]
philly_rentals[,id := .I]
philly_rentals[,pm.zip := coalesce(pm.zip, str_sub(zip,1,5)) %>%
                 str_pad(5, "left", pad = "0")]

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

philly_evict[,pm.zip := coalesce(pm.zip,GEOID.Zip) %>%
               str_pad(5, "left", pad = "0")]

philly_evict_address_agg = philly_evict[commercial ==F& dup == F & include == T & !is.na(n_sn_ss_c),list(
  num_evict = .N
), by = .(xfileyear, n_sn_ss_c, pm.house, pm.street,pm.zip,
          pm.streetSuf, pm.sufDir, pm.preDir, GEOID.longitude, GEOID.latitude)]

philly_rentals_long_addys = unique(philly_rentals_long, by = "n_sn_ss_c")
philly_evict_addys = unique(philly_evict_address_agg, by = "n_sn_ss_c")

philly_parcels[,PID := str_pad(parcel_number,9, "left",pad = "0") ]
philly_parcel_addys = unique(philly_parcels, by = "n_sn_ss_c")
philly_parcel_rentals_long_addys = unique(rbindlist(list(philly_rentals_long_addys %>%
                                                           select(pm.address:PID, geocode_x, geocode_y),
                                                philly_parcel_addys %>%
                                                  select(pm.address:PID, geocode_x, geocode_y)
                                                ), fill=T),
                                          by = "n_sn_ss_c")

## num st sfx prefix zip ##
# merge the philly_evict_address_agg data back
num_st_sfx_dir_zip_merge = philly_evict_addys %>%
  # merge with address data
  # only let merge with parcels that have units aka residential ones
  merge(philly_parcel_rentals_long_addys[,.( pm.house, pm.street,pm.zip,
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


#### summary stats on merge ####
# should really make this a function -- it's ugly code.
unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[,.N, by = num_pids][,per := (N / sum(N)) %>% round(2)][order(N)]
unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[,.N, by = .(num_pids==0)][,per := round(N/ sum(N),2)][order(N)]


(unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[num_pids == 0,.N, by = pm.street][
  ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
    order(-N)][1:20])

(unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[!pm.street %in% philly_parcel_rentals_long_addys$pm.street,.N, by = pm.street][
  ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
    order(-N)][1:20])

(unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[num_pids == 0,.N, by = n_sn_ss_c][
  ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
    order(-N)][1:20])


# View(philly_evict_address_agg[!pm.street %in% philly_rentals_long$pm.street] )
#
# View(num_st_sfx_dir_zip_merge[num_pids==2] %>% sample_n(100))
# View(num_st_sfx_dir_zip_merge[num_pids==0] %>% sample_n(100))


#### random sampling ####
# View(num_st_sfx_dir_zip_merge[num_pids > 3 ] %>% sample_n(1000))
# View(num_st_sfx_dir_zip_merge[num_pids == 0 &  pm.street %in% num_st_sfx_dir_zip_merge[,sample(pm.street,100) ]  ] %>%
#        select(-pm.state, -addy_id) %>%
#        arrange(pm.street, pm.house)
#      )


#### merge on num st zip ####

num_st_merge = philly_evict_addys %>%
  filter(
      !n_sn_ss_c %in% matched_num_st_sfx_dir_zip
  ) %>%
  # merge with address data
  # only let merge with parcels that have units aka residential ones
  merge(philly_parcel_rentals_long_addys[,.( pm.house, pm.street,pm.zip,
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
num_st_sfx_merge = philly_evict_address_agg  %>%
  filter(
    !n_sn_ss_c %in% matched_num_st_sfx_dir_zip &
      !n_sn_ss_c %in% matched_num_st
  ) %>%
  # merge with address data
  # only let merge with parcels that have units aka residential ones
  merge(philly_parcel_rentals_long_addys[,.( pm.house, pm.street,
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
# parcel_sf_subset = philly_parcel_rentals_long_addys %>%
#   filter(!is.na(geocode_x) & !is.na(geocode_y) & !is.na(pm.zip)) %>%
#   filter(PID %in% philly_rentals_long_addys$PID |
#   PID %in% philly_parcel_addys[category_code_description %in% c("MIXED USE",
#                                                                        "MULTI FAMILY",
#                                                                        "SINGLE FAMILY",
#                                                                        "APARTMENTS  > 4 UNITS"),PID])
#
#
# # annoying thing that philly has coordinates not polygons ...
# # so i guess do nearest distance?
#
# spatial_join_sf_join = merge(
#   spatial_join,
#   parcel_sf_subset,
#   by = "pm.zip",
#   all.x = T,
#   allow.cartesian = T,
#   suffixes = c("_1", "_2")
# )
#
# setDT(spatial_join_sf_join)
#
# dtHaversine <- function(lat_from, lon_from, lat_to, lon_to, r = 6378137){
#   radians <- pi/180
#   lat_to <- lat_to * radians
#   lat_from <- lat_from * radians
#   lon_to <- lon_to * radians
#   lon_from <- lon_from * radians
#   dLat <- (lat_to - lat_from)
#   dLon <- (lon_to - lon_from)
#   a <- (sin(dLat/2)^2) + (cos(lat_from) * cos(lat_to)) * (sin(dLon/2)^2)
#   return(2 * atan2(sqrt(a), sqrt(1 - a)) * r)
# }
#
# spatial_join_sf_join[,distance := dtHaversine(geocode_y,geocode_x,GEOID.latitude,GEOID.longitude)]
# spatial_join_sf_join[,min_distance := min(distance), by = n_sn_ss_c_1]
# spatial_join_sf_join[,num_within_5 := uniqueN(PID_2[distance < 5]), by = n_sn_ss_c_1]
# spatial_join_sf_join[,num_within_10 := uniqueN(PID_2[distance < 10]), by = n_sn_ss_c_1]
# spatial_join_sf_join[,num_within_25 := uniqueN(PID_2[distance < 25]), by = n_sn_ss_c_1]
# spatial_join_sf_join[,num_within_50 := uniqueN(PID_2[distance < 50]), by = n_sn_ss_c_1]
# spatial_join_sf_join[,which_min_pid := first(PID_2[distance == min(distance)]), by = n_sn_ss_c_1]
#
# spatial_join_sf_join[,num_pids_st := pmin(
#   fifelse(num_within_50==0, 100000, num_within_50),
#   fifelse(num_within_25==0, 100000, num_within_25),
#   fifelse(num_within_10==0, 100000, num_within_10),
#   fifelse(num_within_5==0, 100000, num_within_5))
#   ]
# spatial_join_sf_join[,num_pids_st := fifelse((num_pids_st==100000), 0, num_pids_st)]
# #View(spatial_join_sf_join[n_sn_ss_c_1 == n_sn_ss_c_2 &  n_sn_ss_c_1!=""] %>% relocate(contains("n_sn"),num_within_1e10, distance, GEOID.longitude,geocode_x,GEOID.latitude , geocode_y,PID_2 ))
# # keep unique matches
# spatial_join_sf_join[, matched := num_pids_st == 1 ]
# matched_spatial = spatial_join_sf_join[matched == T, n_sn_ss_c_1]
#
# ## summary stats ##
# #spatial_join_sf_join[,num_pids_st := uniqueN(PID_2,na.rm = T), by = n_sn_ss_c_1]
# unique(spatial_join_sf_join, by = "n_sn_ss_c_1")[,.N, by = .(num_pids_st==0)][,per := round(N/ sum(N),2)][order(N)]
# unique(spatial_join_sf_join, by = "n_sn_ss_c_1")[,.N, by = num_pids_st][,per := (N / sum(N)) %>% round(2)][order(N)]
#
# spatial_join_sf_join[num_pids_st == 0,.N, by = n_sn_ss_c_1][
#   ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
#     order(-N)][1:20]


matched_ids = c(
  matched_num_st_sfx_dir_zip,
  matched_num_st,
  matched_num_st_sfx
  #matched_spatial
)


#unique(spatial_join_sf_join, by = "n_sn_ss_c_1")[,.N, by = .(n_sn_ss_c_1 %in% (matched))][,per := N / sum(N)][]

#                      )
#matched_new = matched_ids
unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[,.N, by = .(n_sn_ss_c %in% (matched_ids))][,per := N / sum(N)][]
#unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[,.N, by = .(n_sn_ss_c %in% (matched_new))][,per := N / sum(N)][]


#### make philly_evict_address_agg-parcels xwalk ####
# first start with parcels that merged uniquely
# spatial_join_sf_join = spatial_join_sf_join %>%
#   mutate(n_sn_ss_c = n_sn_ss_c_1,
#          merge = "spatial"
#          )
xwalk_unique = bind_rows(list(
  num_st_sfx_dir_zip_merge[num_pids == 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  num_st_merge[num_pids == 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  num_st_sfx_merge[num_pids == 1, .(PID,n_sn_ss_c, merge)] %>% distinct()
  # spatial_join_sf_join[num_pids_st == 1 & PID_2 == which_min_pid, .(PID_2,n_sn_ss_c, merge)]  %>%
  #   distinct()%>%
  #   rename(PID = PID_2)
  #fuzzy_match[num_matches==1, .(PID_match, n_sn_ss_c, merge)] %>% rename(PID = PID_match) %>% distinct()
)
) %>%
  mutate(unique = T) %>%
  as.data.table()

# next append parcels that did not merge uniquely
xwalk_non_unique = bind_rows(list(
  num_st_sfx_dir_zip_merge[num_pids > 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  num_st_merge[num_pids > 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  num_st_sfx_merge[num_pids > 1, .(PID,n_sn_ss_c, merge)] %>% distinct()
  # spatial_join_sf_join[num_pids_st > 1 & !is.na(PID_2) & distance < 50, .(PID_2,n_sn_ss_c, merge)]  %>%
  #   distinct()%>%
  #   rename(PID = PID_2)
  #fuzzy_match[num_matches > 1, .(PID_match, n_sn_ss_c, merge)] %>% rename(PID = PID_match) %>% distinct()
)) %>%
  mutate(unique = F) %>%
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
unique(xwalk, by = "n_sn_ss_c")[,.N, by = .(merge)][,per := round(N / sum(N),2)][]
xwalk[unique == T,.N, by = num_merges] # this should always be one
xwalk[unique == F,.N, by = num_merges] # this should mostly be one
xwalk[,num_parcels_matched := uniqueN(PID,na.rm = T), by = n_sn_ss_c]
xwalk[,num_addys_matched := uniqueN(n_sn_ss_c,na.rm = T), by = PID]
unique(xwalk[], by = "n_sn_ss_c")[,.N, by = num_parcels_matched][,per := round(N / sum(N),4)][order(num_parcels_matched)]
unique(xwalk[], by = "n_sn_ss_c")[,.N, by = num_addys_matched][,per := round(N / sum(N),4)][order(num_addys_matched)]


#xwalk = unique(xwalk, by = c("n_sn_ss_c", "PID"))
philly_evict_address_agg[,sum(num_evict), by = .(!n_sn_ss_c %in% xwalk_non_unique$n_sn_ss_c &
                           !n_sn_ss_c %in% xwalk_unique$n_sn_ss_c)][,per := V1 / sum(V1)][]



#### export ####
fwrite(xwalk, "/Users/joefish/Desktop/data/philly-evict/philly_evict_address_agg_xwalk.csv")
fwrite(philly_evict_address_agg, "/Users/joefish/Desktop/data/philly-evict/philly_evict_address_agg.csv")


## merge rental listings and evictions on xwalk
philly_rentals_long= philly_rentals_long %>% distinct(PID, year,.keep_all = T)
philly_rentals_evict_m = philly_rentals_long[PID != "" & !is.na(PID)] %>%
  merge(xwalk[!is.na(PID) & PID != "" & num_parcels_matched ==1 ], by = "PID",all.x = T) %>%
  mutate(xfileyear = year,n_sn_ss_c= n_sn_ss_c.y) %>%
  merge(philly_evict_address_agg[!is.na(n_sn_ss_c),list(num_evict = first(num_evict)), by = .(n_sn_ss_c, xfileyear)],
        by = c("n_sn_ss_c","xfileyear"),all.x = T)

philly_rentals_evict_m[,num_evict := fifelse(is.na(num_evict), 0, num_evict)]

# parcels agg
parcels_agg = philly_rentals_evict_m[rentalcategory!= "Hotel",list(
  num_evict = sum(num_evict),
  num_units = first(numberofunits)
  #num_addys = uniqueN(n_sn_ss_c)
), by = .(xfileyear, PID)]

# make sure i get about the same number of evictions
philly_evict_address_agg[,sum(num_evict), by =xfileyear][order(xfileyear)]
philly_rentals_evict_m[,sum(num_evict), by =xfileyear][order(xfileyear)]
parcels_agg[,sum(num_evict), by =xfileyear][order(xfileyear)]
parcels_agg[,sum(num_units), by =xfileyear][order(xfileyear)]

parcels_agg = parcels_agg %>%
  mutate(
    evict_filing_rate = num_evict / num_units
  )%>%
  merge(philly_parcels[,.(PID, category_code_description,owner_1)], by = "PID", all.x = T) %>%
  filter(!category_code_description %in% c("HOTEL","GARAGE - COMMERCIAL","OFFICES",
                                            "VACANT LAND","COMMERCIAL") ) %>%
  # remove student housing
  filter(!str_detect(owner_1, "PHILADELPHIA UNIVERSITY|TEMPLE UNI|DREXEL|UNIVERSITY|UNIV OF")) %>%
  group_by(xfileyear) %>%
  arrange(num_evict) %>%
  mutate(
    cum_per_evict_filings = cumsum(num_evict) / sum(num_evict,na.rm = T),
    cum_per_units = cumsum(num_units) / sum(num_units,na.rm = T),
    evict_ranking = 100*row_number() / n()
  ) %>%
  ungroup() %>%
  as.data.table()

ggplot(parcels_agg[xfileyear %in% c(2016,2019,2022,2023)  ],
       aes(x = evict_ranking,
           y = cum_per_evict_filings)
       #    color = as.factor(xfileyear))
       ) +
  #geom_line(aes(color = "2019")) +
  geom_point( aes(color = "Percentage of Evictions")) +
  geom_point(aes(y = cum_per_units,color = "Percentage of Units"), size = 1, alpha = .5) +
  #geom_point(aes(x = evict_ranking_cur,y = cum_per_evict_filings_cur,color = "2023")) +
  # scale_x_continuous(limits = c(0,100), breaks = seq(0,100,5)) +
  # scale_y_continuous(limits = c(0,1), breaks = seq(0,1,.1)) +
  labs(
    x = "Parcel ranking by evictions",
    y = "Cumulative P",
    title = "Cumulative distribution of evictions and units by Parcel",
    subtitle = "Philadelphia, 2019"
  ) +
  theme_bw() +
  facet_wrap(~xfileyear)

ggsave("figs/cumulative_evict_dist_parcels.png", width = 10, height = 10, bg="white")

ggplot(parcels_agg[xfileyear %in% c(2016,2019,2022,2023) & num_evict == 0  ],
       aes(x = evict_ranking,
           color = as.factor(xfileyear),
           y = cum_per_evict_filings)
       #    color = as.factor(xfileyear))
) +
  #geom_line(aes(color = "2019")) +
  #geom_point() +
  geom_point(aes(y = cum_per_units), size = 1, alpha = .5) +
  #geom_point(aes(x = evict_ranking_cur,y = cum_per_evict_filings_cur,color = "2023")) +
  # scale_x_continuous(limits = c(0,100), breaks = seq(0,100,5)) +
  # scale_y_continuous(limits = c(0,1), breaks = seq(0,1,.1)) +
  labs(
    x = "Parcel ranking by evictions",
    y = "Cumulative P",
    title = "Cumulative distribution of evictions and units by Parcel",
    subtitle = "Philadelphia, 2019"
  ) +
  theme_bw()


parcels_agg_wide = pivot_wider(parcels_agg[,num_evict := replace_na(num_evict,0)][,.(PID, num_evict, evict_filing_rate,xfileyear)],
                               names_from = xfileyear, values_from = c(num_evict, evict_filing_rate))
setDT(parcels_agg_wide)
ggplot(parcels_agg_wide[],
       aes(x = replace_na(num_evict_2019,0), y = replace_na(num_evict_2023,0))) +
  geom_point() +
  geom_abline()+
  #geom_smooth() +
  labs(
    x = "Number of evictions (2019)",
    y = "Number of evictions (2023)",
    #title = "Evictions by number of units",
    #subtitle = "Philadelphia, 2019"
  ) +
  theme_bw()

ggplot(parcels_agg_wide[num_evict_2019 > 0 & num_evict_2023 > 0],
       aes(x = evict_filing_rate_2019, y = (evict_filing_rate_2023))) +
  geom_point() +
  geom_smooth() +
  labs(
    x = "Number of units (2019)",
    y = "Number of evictions (2023)",
    title = "Evictions by number of units",
    subtitle = "Philadelphia, 2019"
  ) +
  theme_bw()






