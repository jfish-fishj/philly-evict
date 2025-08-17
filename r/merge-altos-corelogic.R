library(data.table)
library(tidyverse)
#library(sf)
#library(tidycensus)
library(glue)
#### files ####
source("r/helper-functions.R")

corelogic_input_dir = '~/Desktop/data/corelogic/cleaned-addresses/'
altos_input_dir = "~/Desktop/data/altos/cities/"
#fips_code = '39035' # Philadelphia County FIPS code
#metro = "cleveland"
outdir = '/Users/joefish/Desktop/data/altos-corelogic-xwalk'

# loop through metros and make xwalk

for(row in (1:nrow(cities_fips))){
  metro = cities_fips$city[row]
  fips_code = cities_fips$fips[row]
  print(glue("\nProcessing {metro} with fips {fips_code}\n"))

   # cities_fips sourced from helper-functions.R

    outfile_xwalk = glue("{outdir}/{metro}_{fips_code}_altos_corelogic_xwalk.csv")
    outfile_xwalk_listing = glue("{outdir}/{metro}_{fips_code}_altos_corelogic_xwalk_listing.csv")
    # check if outfile_xwalk file exists; if yes, skip
    if(file.exists(outfile_xwalk) & file.exists(outfile_xwalk_listing)){
      cat(glue("\nSkipping {metro} since xwalk file already exists\n"))
      next
    }
    # skip miami
    if(metro == "miami"){
      cat("\nSkipping miami\n")
      next
    }
    #### read in altos data ####
    altos_file = glue("{altos_input_dir}{metro}_metro_all_years_clean_addys.csv")
    corelogic_file = glue("{corelogic_input_dir}/cleaned_{fips_code}.csv")
    if(!file.exists(altos_file) | !file.exists(corelogic_file)){
      stop(glue("Files {altos_file} or {corelogic_file} do not exist."))
    }
    altos = fread(altos_file)
    altos[,pm.zip_imp := paste0("_",str_pad(zip, 5,"left","0")) ] # just take the first 6 of the zip code
    corelogic = fread(corelogic_file)
    corelogic[,pm.zip_imp := str_sub(pm.zip_imp,1,6)]
    corelogic = corelogic %>% mutate(across(contains("dir"), ~str_to_lower(.x)))
    altos_addys = altos %>% unique(by = "n_sn_ss_c")
    corelogic_addys = unique(corelogic,by = "n_sn_ss_c")

    ## num st sfx prefix zip ##
    # merge the philly_altos_address_agg data back
    num_st_sfx_dir_zip_merge = altos_addys %>%
      # merge with address data
      # only let merge with parcels that have units aka residential ones
      merge(corelogic_addys[,.( pm.house, pm.street,pm.zip_imp,
                                                 pm.streetSuf,
                                pm.dir_concat,clip
      )],
      ,by = c("pm.house", "pm.street", "pm.streetSuf", "pm.dir_concat", "pm.zip_imp")
      ,all.x= T,
      allow.cartesian = T
      ) %>%
      mutate(
        merge = "num_st_sfx_dir_zip"
      )

    setDT(num_st_sfx_dir_zip_merge)
    num_st_sfx_dir_zip_merge[,num_clips := uniqueN(clip,na.rm = T), by = n_sn_ss_c]
    ## save the merges that are unique ##
    num_st_sfx_dir_zip_merge[,matched := num_clips==1]
    matched_num_st_sfx_dir_zip = num_st_sfx_dir_zip_merge[matched == T, n_sn_ss_c]


    #### summary stats on merge ####
    # should really make this a function -- it's ugly code.
    unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[,.N, by = num_clips][,per := (N / sum(N)) %>% round(2)][order(N)]
    unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[,.N, by = .(num_clips==0)][,per := round(N/ sum(N),2)][order(N)]


    (unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[num_clips == 0,.N, by = pm.street][
      ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
        order(-N)][1:20])

    (unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[!pm.street %in% corelogic_addys$pm.street,.N, by = pm.street][
      ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
        order(-N)][1:20])

    (unique(num_st_sfx_dir_zip_merge, by = "n_sn_ss_c")[num_clips == 0,.N, by = n_sn_ss_c][
      ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
        order(-N)][1:20])


    # View(philly_altos_address_agg[!pm.street %in% philly_rentals_long$pm.street] )
    #
    # View(num_st_sfx_dir_zip_merge[num_clips==2] %>% sample_n(100))
    # View(num_st_sfx_dir_zip_merge[num_clips==0] %>% sample_n(100))


    #### random sampling ####
    # View(num_st_sfx_dir_zip_merge[num_clips > 3 ] %>% sample_n(1000))
    # View(num_st_sfx_dir_zip_merge[num_clips == 0 &  pm.street %in% num_st_sfx_dir_zip_merge[,sample(pm.street,100) ]  ] %>%
    #        #select(-pm.state, -addy_id) %>%
    #        arrange(pm.street, pm.house)
    # )


    #### merge on num st zip ####

    num_st_merge = altos_addys %>%
      filter(
        !n_sn_ss_c %in% matched_num_st_sfx_dir_zip
      ) %>%
      # merge with address data
      # only let merge with parcels that have units aka residential ones
      merge(corelogic_addys[,.( pm.house, pm.street,pm.zip_imp,
                                                 pm.streetSuf, pm.sufDir, pm.preDir,clip
      )],
      ,by = c("pm.house", "pm.street",'pm.zip_imp')
      ,all.x= T,
      allow.cartesian = T
      ) %>%
      mutate(
        merge = "num_st"
      )

    setDT(num_st_merge)
    num_st_merge[,num_clips := uniqueN(clip,na.rm = T), by = n_sn_ss_c]

    ## keep the merges that are unique ##
    num_st_merge[,matched := num_clips==1]
    matched_num_st = num_st_merge[matched == T, n_sn_ss_c]

    # check that we just kept unique merges...
    sum( matched_num_st %in%  matched_num_st_sfx_dir_zip) # should be zero

    #### summary stats on merge ####
    # should really make this a function -- it's ugly code.
    unique(num_st_merge, by = "n_sn_ss_c")[,.N, by = num_clips][,per := (N / sum(N)) %>% round(2)][order(num_clips)]
    unique(num_st_merge, by = "n_sn_ss_c")[,.N, by = .(num_clips==0)][,per := round(N/ sum(N),2)][order(num_clips)]
    (unique(num_st_merge, by = "n_sn_ss_c")[num_clips == 0,.N, by = pm.street][
      ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
        order(-N)][1:20])

    unique(num_st_merge, by = "n_sn_ss_c")[,.N, by = .(num_clips==0)][,per := round(N/ sum(N),2)][order(num_clips)]
    (unique(num_st_merge, by = "n_sn_ss_c")[num_clips == 0,.N, by = n_sn_ss_c][
      ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
        order(-N)][1:20])


    #### num st sfx merge ####
    num_st_sfx_merge = altos_addys  %>%
      filter(
        !n_sn_ss_c %in% matched_num_st_sfx_dir_zip &
          !n_sn_ss_c %in% matched_num_st
      ) %>%
      # merge with address data
      # only let merge with parcels that have units aka residential ones
      merge(corelogic_addys[,.( pm.house, pm.street,
                                                 pm.streetSuf,clip
      )],
      ,by = c("pm.house", "pm.street","pm.streetSuf")
      ,all.x= T,
      allow.cartesian = T
      ) %>%
      mutate(
        merge = "num_st_sfx"
      )


    setDT(num_st_sfx_merge)
    num_st_sfx_merge[,num_clips := uniqueN(clip,na.rm = T), by = n_sn_ss_c]

    ## keep the merges that are unique ##
    num_st_sfx_merge[,matched := num_clips==1]
    matched_num_st_sfx = num_st_sfx_merge[matched == T, unique(n_sn_ss_c)]
    sum(matched_num_st_sfx %in% matched_num_st)

    #### summary stats on merge ####
    # should really make this a function -- it's ugly code.
    unique(num_st_sfx_merge, by = "n_sn_ss_c")[,.N, by = num_clips][,per := (N / sum(N)) %>% round(2)][order(num_clips)]
    unique(num_st_sfx_merge, by = "n_sn_ss_c")[,.N, by = .(num_clips==0)][,per := round(N/ sum(N),2)][order(num_clips)]
    unique(num_st_sfx_merge, by = "n_sn_ss_c")[num_clips == 0,.N, by = pm.street][
      ,per :=N / sum(N)][order(-N),cum_p := cumsum(per)][
        order(-N)][1:20]


    #### spatial join ####
    # # not doing this for now since it's parcel centroid not polygons
    # in the future, i could do this with a distance based approach, but that seems maybe not worht
    # the effort right now
    # # set this off otherwise the spatial merge doesnt work for whatever reason
    # sf_use_s2(FALSE)
    # # filter to just be parcels that didn't uniquely merge
    # # spatial_join = num_st_sfx_merge[num_clips != 1]
    # #
    # corelogic_sf = corelogic_addys %>%
    #   filter(!is.na(parcel_level_longitude) & !is.na(parcel_level_latitude)) %>%
    #   st_as_sf(coords = c("parcel_level_longitude", "parcel_level_latitude"))
    #
    # corelogic_sf_subset = corelogic_sf %>%
    #   filter(clip %in% corelogic[!property_indicator_code %in% c(
    #     20,23:90 # commercial property codes
    #   ), clip])
    #
    # # filter to just be parcels that didn't uniquely merge
    # spatial_join = num_st_sfx_merge[num_clips != 1]
    #
    # # make altos into shape file
    # # note here that altos data is only geocoded to 5 digits or within a meter
    # # of precission.
    # # also pretty sure that it's geocoding to the street level in a lot of cases
    # # so there might be some room to re geocode them and get parcel-level coordinates
    # spatial_join_sf = spatial_join %>%
    #   filter(!is.na(geo_long) & !is.na(geo_lat)) %>%
    #   st_as_sf(coords = c("geo_long", "geo_lat")) %>%
    #   st_set_crs(value = st_crs(corelogic_sf_subset))
    #
    # spatial_join_sf_join = st_join(
    #   spatial_join_sf,
    #   corelogic_sf_subset %>%
    #     rename(clip_2 = clip) %>%
    #     select(clip_2, geometry)# %>% filter(clip %in% fa_expand[, clip])
    # )
    #
    # spatial_join_sf_join = spatial_join_sf_join %>%
    #   as.data.table() %>%
    #   select(-geometry) %>%
    #   mutate(merge = "spatial")
    #
    # spatial_join_sf_join[,num_clips_st := uniqueN(clip_2), by = n_sn_ss_c]
    #
    # # keep unique matches
    # spatial_join_sf_join[, matched := num_clips_st == 1 ]
    # matched_spatial = spatial_join_sf_join[matched == T, n_sn_ss_c]
    #
    # ## summary stats ##
    # spatial_join_sf_join[,num_clips_st := uniqueN(clip_2,na.rm = T), by = n_sn_ss_c]
    # unique(spatial_join_sf_join, by = "n_sn_ss_c")[,.N, by = .(num_clips_st==0)][,per := round(N/ sum(N),2)][order(N)]
    # unique(spatial_join_sf_join, by = "n_sn_ss_c")[,.N, by = num_clips_st][,per := (N / sum(N)) %>% round(2)][order(N)]
    #
    # spatial_join_sf_join[num_clips_st == 0,.N, by = pm.street][
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


    #### make philly_altos_address_agg-parcels xwalk ####
    # first start with parcels that merged uniquely
    # spatial_join_sf_join = spatial_join_sf_join %>%
    #   mutate(n_sn_ss_c = n_sn_ss_c_1,
    #          merge = "spatial"
    #          )
    xwalk_unique = bind_rows(list(
      num_st_sfx_dir_zip_merge[num_clips == 1, .(clip,n_sn_ss_c, merge)] %>% distinct(),
      num_st_merge[num_clips == 1, .(clip,n_sn_ss_c, merge)] %>% distinct(),
      num_st_sfx_merge[num_clips == 1, .(clip,n_sn_ss_c, merge)] %>% distinct()
      # spatial_join_sf_join[num_clips_st == 1, .(clip_2,n_sn_ss_c, merge)]  %>%
      #   distinct()%>%
      #   rename(clip = clip_2)
      #fuzzy_match[num_matches==1, .(clip_match, n_sn_ss_c, merge)] %>% rename(clip = clip_match) %>% distinct()
    )
    ) %>%
      mutate(unique = T) %>%
      as.data.table()

    # next append parcels that did not merge uniquely
    xwalk_non_unique = bind_rows(list(
      num_st_sfx_dir_zip_merge[num_clips > 1, .(clip,n_sn_ss_c, merge)] %>% distinct(),
      num_st_merge[num_clips > 1, .(clip,n_sn_ss_c, merge)] %>% distinct(),
      num_st_sfx_merge[num_clips > 1, .(clip,n_sn_ss_c, merge)] %>% distinct()
      # spatial_join_sf_join[num_clips_st > 1 & !is.na(clip_2), .(clip_2,n_sn_ss_c, merge)]  %>%
      #   distinct()%>%
      #   rename(clip = clip_2)
      #fuzzy_match[num_matches > 1, .(clip_match, n_sn_ss_c, merge)] %>% rename(clip = clip_match) %>% distinct()
    )) %>%
      mutate(unique = F) %>%
      filter(!n_sn_ss_c %in% xwalk_unique$n_sn_ss_c) %>%
      filter(!is.na(clip)) %>%
      distinct(clip, n_sn_ss_c,.keep_all = T) %>%
      as.data.table()


    xwalk = bind_rows(list(
      xwalk_unique,
      xwalk_non_unique,
      tibble(n_sn_ss_c = altos_addys[(!n_sn_ss_c %in% xwalk_non_unique$n_sn_ss_c &
                                                     !n_sn_ss_c %in% xwalk_unique$n_sn_ss_c), n_sn_ss_c],
             merge = "not merged", clip = NA) %>% distinct()
    ) )

    xwalk[,num_merges := uniqueN(merge), by = .(n_sn_ss_c, unique)]
    unique(xwalk, by = "n_sn_ss_c")[,.N, by = .(merge)][,per := round(N / sum(N),2)][]
    xwalk[unique == T,.N, by = num_merges] # this should always be one
    xwalk[unique == F,.N, by = num_merges] # this should mostly be one
    xwalk[,num_parcels_matched := uniqueN(clip,na.rm = T), by = n_sn_ss_c]
    xwalk[,num_addys_matched := uniqueN(n_sn_ss_c,na.rm = T), by = clip]
    unique(xwalk[], by = "n_sn_ss_c")[,.N, by = num_parcels_matched][,per := round(N / sum(N),4)][order(num_parcels_matched)]
    unique(xwalk[], by = "n_sn_ss_c")[,.N, by = num_addys_matched][,per := round(N / sum(N),4)][order(num_addys_matched)]


    #xwalk = unique(xwalk, by = c("n_sn_ss_c", "clip"))
    altos_addys[,sum(num_listings), by = .(!n_sn_ss_c %in% xwalk_non_unique$n_sn_ss_c &
                                                          !n_sn_ss_c %in% xwalk_unique$n_sn_ss_c)][,per := V1 / sum(V1)][]

    xwalk_listing = merge(
      xwalk,
      altos[,.(n_sn_ss_c, listing_id, city)] %>% distinct(),
      by = "n_sn_ss_c", allow.cartesian = T
    )

    # summary stats by city
    xwalk_listing[, list(sum(unique,na.rm= T),.N), by = .(city)][,per := round(V1 / N,2)][order(desc(N))][1:10] %>%
      print()


    #### export ####
    fwrite(xwalk, outfile_xwalk)
    fwrite(xwalk_listing,outfile_xwalk_listing)

}
