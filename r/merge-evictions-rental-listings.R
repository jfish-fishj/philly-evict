library(data.table)
library(tidyverse)
library(sf)
library(tidycensus)

philly_lic = fread("/Users/joefish/Desktop/data/philly-evict/business_licenses_clean.csv")
philly_evict =  fread("/Users/joefish/Desktop/data/philly-evict/evict_address_cleaned.csv")
philly_def_names <- fread("~/Desktop/data/philly-evict/phila-lt-data/names_merge_defendants.csv")
#philly_evict_addys = fread("/Users/joefish/Desktop/data/philly-evict/evict_address_cleaned.csv")
philly_parcels = fread("/Users/joefish/Desktop/data/philly-evict/parcel_address_cleaned.csv")
#rgdal::ogrInfo(system.file("/Users/joefish/Desktop/data/philly-evict/opa_properties_public.gdb", package="sf"))
philly_parcels_sf = read_sf("/Users/joefish/Desktop/data/philly-evict/philly_parcels_sf/DOR_Parcel.shp")

source("~/Documents/GitHub/philly-evictions/r/helper-functions.R")


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

# clean some names
philly_evict[,clean_defendant_name := clean_name(defendant)]
# flag commercial
philly_evict[,commercial_alt := str_detect(clean_defendant_name, business_regex)]
philly_evict[,pm.zip := (pm.zip) %>%
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
philly_parcel_addys[,pm.zip := (pm.zip) %>%
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

## summary stats ##
spatial_join_sf_join[,num_pids_st := uniqueN(PID_2,na.rm = T), by = n_sn_ss_c]
unique(spatial_join_sf_join, by = "n_sn_ss_c")[,.N, by = .(num_pids_st==0)][,per := round(N/ sum(N),2)][order(N)]
unique(spatial_join_sf_join, by = "n_sn_ss_c")[,.N, by = num_pids_st][,per := (N / sum(N)) %>% round(2)][order(N)]

spatial_join_sf_join[num_pids_st == 0,.N, by = pm.street][
  ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
    order(-N)][1:20]



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


#### make philly_evict_address_agg-parcels xwalk ####
# first start with parcels that merged uniquely
spatial_join_sf_join = spatial_join_sf_join %>%
  mutate(n_sn_ss_c = n_sn_ss_c,
         merge = "spatial"
         )
xwalk_unique = bind_rows(list(
  num_st_sfx_dir_zip_merge[num_pids == 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  num_st_merge[num_pids == 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  num_st_sfx_merge[num_pids == 1, .(PID,n_sn_ss_c, merge)] %>% distinct(),
  spatial_join_sf_join[num_pids_st == 1 , .(PID_2,n_sn_ss_c, merge)]  %>%
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
  spatial_join_sf_join[num_pids_st > 1 , .(PID_2,n_sn_ss_c, merge)]  %>%
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

xwalk_case = merge(
  xwalk,
  philly_evict[n_sn_ss_c != "",.(n_sn_ss_c, id)],
  by = "n_sn_ss_c"
)

xwalk_case[,.N, by = num_parcels_matched][,per := round(N / sum(N),4)][order(num_parcels_matched)]


#### export ####
fwrite(xwalk, "/Users/joefish/Desktop/data/philly-evict/philly_evict_address_agg_xwalk.csv")
fwrite(philly_evict_address_agg, "/Users/joefish/Desktop/data/philly-evict/philly_evict_address_agg.csv")
fwrite(xwalk_case, "/Users/joefish/Desktop/data/philly-evict/philly_evict_address_agg_xwalk_case.csv")

xwalk = fread("/Users/joefish/Desktop/data/philly-evict/philly_evict_address_agg_xwalk.csv")
xwalk[,PID := str_pad(as.character(PID),9, "left","0")]
philly_evict_address_agg = fread("/Users/joefish/Desktop/data/philly-evict/philly_evict_address_agg.csv")
## merge rental listings and evictions on xwalk
philly_rentals_long = philly_rentals_long %>% distinct(PID, year,.keep_all = T)
philly_rentals_evict_m = philly_rentals_long[PID != "" & !is.na(PID)] %>%
  merge(xwalk[!is.na(PID) & PID != "" & num_parcels_matched ==1 ], by = "PID",all.x = T) %>%
  mutate(year = year,n_sn_ss_c= n_sn_ss_c.y) %>%
  merge(philly_evict_address_agg[!is.na(n_sn_ss_c),list(num_evict = first(num_evict)), by = .(n_sn_ss_c, year)],
        by = c("n_sn_ss_c","year"),all.x = T)

philly_rentals_evict_m[,num_evict := fifelse(is.na(num_evict), 0, num_evict)]

# parcels agg
parcels_agg = philly_rentals_evict_m[rentalcategory!= "Hotel",list(
  num_evict = sum(num_evict),
  num_units = first(numberofunits)
  #num_addys = uniqueN(n_sn_ss_c)
), by = .(year, PID)]

# make sure i get about the same number of evictions
philly_evict_address_agg[,sum(num_evict), by =year][order(year)] %>%
  merge(parcels_agg[,sum(num_evict), by =year][order(year)], by = "year") %>%
  mutate(per = V1.y/V1.x)

philly_rentals_evict_m[,sum(num_evict), by =year][order(year)]
parcels_agg[,sum(num_evict), by =year][order(year)]
parcels_agg[,sum(num_units), by =year][order(year)]

parcels_agg = parcels_agg %>%
  mutate(
    evict_filing_rate = num_evict / num_units
  )%>%
  merge(philly_parcels[,.(PID, category_code_description,owner_1,mailing_street, pm.zip)], by = "PID", all.x = T) %>%
  filter(!category_code_description %in% c("HOTEL","GARAGE - COMMERCIAL","OFFICES",
                                            "VACANT LAND","COMMERCIAL") ) %>%
  # remove student housing
  filter(!str_detect(owner_1, "PHILADELPHIA UNIVERSITY|TEMPLE UNI|DREXEL|UNIVERSITY|UNIV OF")) %>%
  group_by(year) %>%
  arrange(num_evict) %>%
  mutate(
    cum_per_evict_filings = cumsum(num_evict) / sum(num_evict,na.rm = T),
    cum_per_units = cumsum(num_units) / sum(num_units,na.rm = T),
    evict_ranking = 100*row_number() / n()
  ) %>%
  ungroup() %>%
  as.data.table()

breaks <- c(seq(0, 95, by = 10), seq(99, 100, by = 0.1))
coarse_breaks <- seq(0, 99, by = 10)
fine_breaks <- seq(95, 100, by = 1)
# Custom transformation
transform_x <- function(x) {
  ifelse(x <= 99, x, 99 + (x - 99) * 10) # Stretch 95-100 range
}

inverse_transform_x <- function(x) {
  ifelse(x <= 99, x, 99 + (x - 99) / 10) # Inverse for correct labeling
}


ggplot(parcels_agg[year %in% c(2019)  ] ,
       aes(x = evict_ranking,
           y = cum_per_evict_filings)
       #    color = as.factor(year))
       ) +
  #geom_line(aes(color = "2019")) +
  geom_point( aes(color = "Percentage of Evictions")) +
  #geom_point(aes(y = cum_per_units,color = "Percentage of Units"), size = 1, alpha = .5) +
  #geom_point(aes(x = evict_ranking_cur,y = cum_per_evict_filings_cur,color = "2023")) +
  # scale_x_continuous(limits = c(0,100), breaks = seq(0,100,5)) +
  # scale_y_continuous(limits = c(0,1), breaks = seq(0,1,.1)) +
  labs(
    x = "Parcel ranking by evictions",
    y = "Cumulative P",
    title = "Cumulative distribution of evictions and units by Parcel",
    subtitle = "Philadelphia, 2019"
  ) +

  #coord_cartesian(xlim = c(0, 100)) +
  theme_bw()

ggsave("figs/cumulative_evict_dist_parcels.png", width = 10, height = 10, bg="white")

ggplot(parcels_agg[year %in% c(2016,2019,2022,2023) & num_evict == 0  ],
       aes(x = evict_ranking,
           color = as.factor(year),
           y = cum_per_evict_filings)
       #    color = as.factor(year))
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


parcels_agg_wide = pivot_wider(parcels_agg[,num_evict := replace_na(num_evict,0)][,.(PID, num_evict, evict_filing_rate,year)],
                               names_from = year, values_from = c(num_evict, evict_filing_rate))
setDT(parcels_agg_wide)
ggplot(parcels_agg_wide[],
       aes(x = replace_na(num_evict_2019,0), y = replace_na(num_evict_2023,0))) +
  geom_point() +
  geom_abline()+
  geom_smooth() +
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






