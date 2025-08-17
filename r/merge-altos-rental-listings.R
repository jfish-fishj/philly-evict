library(data.table)
library(tidyverse)
library(sf)
library(tidycensus)

philly_lic = fread("/Users/joefish/Desktop/data/philly-evict/business_licenses_clean.csv")
philly_altos = fread("~/Desktop/data/altos/cities/philadelphia_metro_all_years_clean_addys.csv")
philly_parcels = fread("/Users/joefish/Desktop/data/philly-evict/parcel_address_cleaned.csv")
philly_parcels_sf = read_sf("/Users/joefish/Desktop/data/philly-evict/philly_parcels_sf/DOR_Parcel.shp")

#rgdal::ogrInfo(system.file("/Users/joefish/Desktop/data/philly-evict/opa_properties_public.gdb", package="sf"))

philly_altos[,listing_id := seq(.N)]

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
philly_rentals_long[,PID :=opa_account_num ]
philly_rentals_long = philly_rentals_long[year %in% 2016:2024 & n_sn_ss_c != ""]
philly_parcels[,PID:= str_pad(parcel_number,9, "left",pad = "0")]
philly_parcels_sf_m = philly_parcels_sf %>% select(-matches("PID")) %>%
  merge(philly_parcels[,.(pin, PID)], by.x = "PIN", by.y = "pin")

# now add in the

philly_altos[,pm.zip := coalesce(as.character(zip),pm.zip) %>%
               str_pad(5, "left", pad = "0")]

philly_altos[,GEOID.longitude := as.numeric(geo_long)]
philly_altos[,GEOID.latitude := as.numeric(geo_lat)]


philly_altos_address_agg = philly_altos[,list(
  num_listings = .N
), by = .(year, n_sn_ss_c, pm.house, pm.street,pm.zip,
          pm.streetSuf, pm.sufDir, pm.preDir, GEOID.longitude, GEOID.latitude)]

philly_rentals_long_addys = unique(philly_rentals_long, by = "n_sn_ss_c")
philly_altos_addys = unique(philly_altos_address_agg, by = "n_sn_ss_c")

philly_parcels[,PID := str_pad(parcel_number,9, "left",pad = "0") ]
philly_parcel_addys = unique(philly_parcels, by = "n_sn_ss_c")
philly_parcel_addys[,pm.zip := as.character(pm.zip)]
# philly_parcel_addys = unique(rbindlist(list(philly_rentals_long_addys %>%
#                                                            select(pm.address:PID, geocode_x, geocode_y),
#                                                 philly_parcel_addys %>%
#                                                   select(pm.address:PID, geocode_x, geocode_y)
#                                                 ), fill=T),
#                                           by = "n_sn_ss_c")

## num st sfx prefix zip ##
# merge the philly_altos_address_agg data back
num_st_sfx_dir_zip_merge = philly_altos_addys %>%
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


# View(philly_altos_address_agg[!pm.street %in% philly_rentals_long$pm.street] )
#
# View(num_st_sfx_dir_zip_merge[num_pids==2] %>% sample_n(100))
# View(num_st_sfx_dir_zip_merge[num_pids==0] %>% sample_n(100))


#### random sampling ####
# View(num_st_sfx_dir_zip_merge[num_pids > 3 ] %>% sample_n(1000))
View(num_st_sfx_dir_zip_merge[num_pids == 0 &  pm.street %in% num_st_sfx_dir_zip_merge[,sample(pm.street,100) ]  ] %>%
       #select(-pm.state, -addy_id) %>%
       arrange(pm.street, pm.house)
     )


#### merge on num st zip ####

num_st_merge = philly_altos_addys %>%
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
num_st_sfx_merge = philly_altos_address_agg  %>%
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
parcel_sf_subset = philly_parcels_sf_m %>%
  filter(PID %in% philly_rentals_long_addys$PID |
  PID %in% philly_parcel_addys[category_code_description %in% c("MIXED USE",
                                                                       "MULTI FAMILY",
                                                                       "SINGLE FAMILY",
                                                                       "APARTMENTS  > 4 UNITS"),PID])

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


matched_ids = c(
  matched_num_st_sfx_dir_zip,
  matched_num_st,
  matched_num_st_sfx,
  matched_spatial
)


#unique(spatial_join_sf_join, by = "n_sn_ss_c_1")[,.N, by = .(n_sn_ss_c_1 %in% (matched))][,per := N / sum(N)][]

#                      )
#matched_new = matched_ids
unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[,.N, by = .(n_sn_ss_c %in% (matched_ids))][,per := N / sum(N)][]
#unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[,.N, by = .(n_sn_ss_c %in% (matched_new))][,per := N / sum(N)][]

# take some random samples of not merged

#### make philly_altos_address_agg-parcels xwalk ####
# first start with parcels that merged uniquely
# spatial_join_sf_join = spatial_join_sf_join %>%
#   mutate(n_sn_ss_c = n_sn_ss_c_1,
#          merge = "spatial"
#          )
xwalk_unique = bind_rows(list(
  num_st_sfx_dir_zip_merge[num_pids == 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  num_st_merge[num_pids == 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  num_st_sfx_merge[num_pids == 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  spatial_join_sf_join[num_pids_st == 1, .(PID_2,n_sn_ss_c, merge)]  %>%
    distinct()%>%
    rename(PID = PID_2)
  #fuzzy_match[num_matches==1, .(PID_match, n_sn_ss_c, merge)] %>% rename(PID = PID_match) %>% distinct()
)
) %>%
  mutate(unique = T) %>%
  as.data.table()

# next append parcels that did not merge uniquely
xwalk_non_unique = bind_rows(list(
  num_st_sfx_dir_zip_merge[num_pids > 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  num_st_merge[num_pids > 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  num_st_sfx_merge[num_pids > 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  spatial_join_sf_join[num_pids_st > 1 & !is.na(PID_2), .(PID_2,n_sn_ss_c, merge)]  %>%
    distinct()%>%
    rename(PID = PID_2)
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
  tibble(n_sn_ss_c = philly_altos_address_agg[(!n_sn_ss_c %in% xwalk_non_unique$n_sn_ss_c &
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
xwalk[,.N, by = num_addys_matched][,per := round(N / sum(N),4)][order(num_addys_matched)]


#xwalk = unique(xwalk, by = c("n_sn_ss_c", "PID"))
philly_altos_address_agg[,sum(num_listings), by = .(!n_sn_ss_c %in% xwalk_non_unique$n_sn_ss_c &
                           !n_sn_ss_c %in% xwalk_unique$n_sn_ss_c)][,per := V1 / sum(V1)][]

xwalk_listing = merge(
  xwalk,
  philly_altos[,.(n_sn_ss_c, listing_id)] %>% distinct(),
  by = "n_sn_ss_c", allow.cartesian = T
)

xwalk_listing[,.N, by = num_addys_matched][,per := round(N / sum(N),4)][order(num_addys_matched)]
xwalk_listing[,.N, by = num_parcels_matched][,per := round(N / sum(N),4)][order(num_parcels_matched)]

#### export ####
fwrite(xwalk, "/Users/joefish/Desktop/data/philly-evict/philly_altos_address_agg_xwalk.csv")
fwrite(philly_altos_address_agg, "/Users/joefish/Desktop/data/philly-evict/philly_altos_address_agg.csv")
fwrite(xwalk_listing, "/Users/joefish/Desktop/data/philly-evict/philly_altos_address_agg_xwalk_listing.csv")

