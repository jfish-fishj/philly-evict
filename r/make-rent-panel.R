library(data.table)
library(tidyverse)
library(sf)
library(tidycensus)

xwalk_altos = fread("/Users/joefish/Desktop/data/philly-evict/philly_altos_address_agg_xwalk.csv")
xwalk_altos[,PID := str_pad(as.character(PID),9, "left","0")]

xwalk_evict = fread("/Users/joefish/Desktop/data/philly-evict/philly_evict_address_agg_xwalk.csv")
xwalk_evict[,PID := str_pad(as.character(PID),9, "left","0")]


philly_altos_address_agg = fread("/Users/joefish/Desktop/data/philly-evict/philly_altos_address_agg.csv")
philly_altos_beds = fread("~/Desktop/data/philly-evict/altos_year_bedrooms_philly.csv")
philly_evict = fread("/Users/joefish/Desktop/data/philly-evict/evict_address_cleaned.csv")

philly_lic = fread("/Users/joefish/Desktop/data/philly-evict/business_licenses_clean.csv")
philly_parcels = fread("/Users/joefish/Desktop/data/philly-evict/parcel_address_cleaned.csv")
philly_parcels_sf = read_sf("/Users/joefish/Desktop/data/philly-evict/philly_parcels_sf/DOR_Parcel.shp")

#rgdal::ogrInfo(system.file("/Users/joefish/Desktop/data/philly-evict/opa_properties_public.gdb", package="sf"))

#philly_altos[,listing_id := seq(.N)]

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
philly_rentals_long = philly_rentals_long[year %in% 2011:2024 & n_sn_ss_c != ""]
philly_parcels[,PID:= str_pad(parcel_number,9, "left",pad = "0")]
philly_parcels_sf_m = philly_parcels_sf %>% select(-matches("PID")) %>%
  merge(philly_parcels[,.(pin, PID)], by.x = "PIN", by.y = "pin")

## merge rental listings and evictions on xwalk
philly_altos_beds[,PID := fifelse(!is.na(PID),str_pad(PID,9, "left","0"),NA_character_)]

philly_rentals_long = philly_rentals_long %>% distinct(PID, year,.keep_all = T)

philly_rentals_altos_m = philly_rentals_long[PID != "" & !is.na(PID)] %>%
  merge(philly_altos_beds[PID %in% xwalk_altos[num_parcels_matched==1, PID]],
        by = c("PID","year"),all.x = T)

philly_rentals_altos_m[,list(.N,
                             sum(!is.na(med_price)),
                             median(med_price,na.rm =T),
                             mean(beds_imp_first,na.rm=T)
                             ), by = year][order(year)]

philly_rentals_altos_m[,num_listings := fifelse(is.na(num_listings), 0, num_listings)]

philly_evict_parcel = merge(
  philly_evict[],
  xwalk_evict[num_parcels_matched == 1,.(PID,n_sn_ss_c)],
  by = c("n_sn_ss_c"),
  suffixes = c("","_evict")
)

philly_evict_parcel[,num_months_backrent := fifelse(ongoing_rent> 0, total_rent/ongoing_rent , NA_real_)]
philly_evict[,num_months_backrent := fifelse(ongoing_rent> 0, total_rent/ongoing_rent , NA_real_)]
philly_evict[n_sn_ss_c!="",serial := .N > 1, by = .(n_sn_ss_c, defendant)]
philly_evict[,tenant_id := .GRP, by = .(n_sn_ss_c, defendant)]
philly_evict[,num_evict_plaintiff := .N, by = .(year, plaintiff)]

(philly_evict[commercial=="f"  & publichousing=="f" & serial == F &
               year %in% 2004:2024,
             list(median(num_months_backrent,na.rm = T),.N),
             by = .(str_detect(plaintiff, regex("llc|lp|inc|corp|trust|llp|PARTNERS|venture|ltd|l p|assoc|ass oc|COMP|invest",ignore_case = T)),year)][
               order(year)] )%>%
  ggplot(aes(x = year, y = V1,
             group = str_detect,
             color = str_detect)) +
  geom_point() +
  geom_line() +
  labs(color = "corporate plaintiff",
       y = "Median Months Backrent",
       title = "Filing Thresholds by Landlord Type: Philadelphia",
       subtitle = "Commercial and Housing Auth Cases Removed") +
  theme_minimal() +
  scale_y_continuous(breaks = seq(1,4,0.5), limits = c(1,4))

(philly_evict[commercial=="f"  & publichousing=="f" & serial == F &
                year %in% 2004:2024,
              list(median(num_months_backrent,na.rm = T),.N),
              by = .(num_evict_plaintiff>20,year)][
                order(year)] ) %>%
  ggplot(aes(x = year, y = V1,
             group = num_evict_plaintiff,
             color = num_evict_plaintiff)) +
  geom_point() +
  geom_line() +
  labs(color = "corporate plaintiff",
       y = "Median Months Backrent",
       title = "Filing Thresholds by Landlord Type: Philadelphia",
       subtitle = "Commercial and Housing Auth Cases Removed") +
  theme_minimal() +
  scale_y_continuous(breaks = seq(1,4,0.5), limits = c(1,4))


philly_evict_parcel_agg = philly_evict_parcel[,list(
  n_evictions = .N,
  n_sn_ss_c = first(n_sn_ss_c),
  a = first(a),
  b = first(b),
  c = first(c),
  commercial = first(commercial),
  plaintiff = first(plaintiff),
  plaintiff_atty_name = first(plaintiff_atty_name),
  plaintiff_address = first(plaintiff_address),
  med_amt_sought = median(med_amt_sought),
  mean_amt_sought = mean(med_amt_sought),
  publichousing = first(publichousing),
  mean_

)]

philly_rentals_altos_evict_m = merge(
  philly_rentals_altos_m,
  xwalk_evict[num_parcel_matched == 1,.(PID,n_sn_ss_c)],
  by = "PID", suffixes = c("","_evict")
) %>%
  merge(
    philly_evict[,.(n_sn_ss_c, )],
    by = c("n_sn_ss_c","year"),
    suffixes = c("","_evict")
  )

# parcels agg
parcels_agg = philly_rentals_altos_m[rentalcategory!= "Hotel",list(
  num_listings = sum(num_listings),
  num_units = first(numberofunits)
  #num_addys = uniqueN(n_sn_ss_c)
), by = .(year, PID)]

# make sure i get about the same number of evictions
philly_altos_address_agg[,sum(num_listings), by =year][order(year)]
philly_rentals_altos_m[,sum(num_listings), by =year][order(year)]
parcels_agg[,sum(num_listings), by =year][order(year)]
parcels_agg[,sum(num_units), by =year][order(year)]




