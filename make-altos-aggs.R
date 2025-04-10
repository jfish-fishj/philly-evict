#### libraries
library(data.table)
library(tidyverse)
library(postmastr)
library(bit64)
library(sf)
library(geosphere)
library(DescTools)
library(fixest)

altos = fread('/Users/joefish/Desktop/data/altos/cities/philadelphia_metro_all_years_clean_addys.csv')
xwalk = fread("~/Desktop/data/philly-evict/philly_altos_address_agg_xwalk.csv")
xwalk_unique = xwalk[num_parcels_matched==1 & !is.na(PID)]

#### functions ####
Mode <- function(x, na.rm = FALSE) {
  if(na.rm){
    x = x[!is.na(x)]
  }

  ux <- unique(x)
  return(ux[which.max(tabulate(match(x, ux)))])
}

impute_units <- function(col){
  col = col %>%
    str_remove("\\.0+")
  case_when(
    str_detect(col, regex("studio", ignore_case = T)) ~ 0,
    str_detect(col, regex("(zero|0)[\\s,]?(bd|bed|br[^a-z])", ignore_case = T)) ~ 0,
    str_detect(col, regex("(one|1)[\\s,]?(bd|bed|br[^a-z])", ignore_case = T)) ~ 1,
    str_detect(col, regex("(two|2)[\\s,]?(bd|bed|br[^a-z])", ignore_case = T)) ~ 2,
    str_detect(col, regex("(three|3)[\\s,]?(bd|bed|br[^a-z])", ignore_case = T)) ~ 3,
    str_detect(col, regex("(four|4)[\\s,]?(bd|bed|br[^a-z])", ignore_case = T)) ~ 4,
    str_detect(col, regex("(five|5)[\\s,]?(bd|bed|br[^a-z])", ignore_case = T)) ~ 5,
    TRUE ~ NA_real_

  ) %>% return()
}

impute_baths <- function(col){
  col = col %>%
    str_remove("\\.0+")
  case_when(
    str_detect(col, regex("(zero|0)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 0,
    str_detect(col, regex("(0.5)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 0.5,
    str_detect(col, regex("(one|1)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 1,
    str_detect(col, regex("(1.5)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 1,
    str_detect(col, regex("(two|2)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 2,
    str_detect(col, regex("(2.5)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 2.5,
    str_detect(col, regex("(three|3)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 3,
    str_detect(col, regex("(four|4)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 4,
    str_detect(col, regex("(five|5)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 5,
    TRUE ~ NA_real_

  ) %>% return()
}


# make some altos vars
# first, let's get the number of months a listing has been up for
altos_dates <- str_match(altos$date, "([0-9]{4})-([0-9]{2})-([0-9]{2})")
altos[,year := as.numeric(altos_dates[,2])]
altos[,month := as.numeric(altos_dates[,3])]
altos[,day := as.numeric(altos_dates[,4])]
altos[,ym := year + (month-1)/12]
altos[,ymd := year + (month-1)/12 + (day-1) / 365]
altos[,price :=as.numeric(price)]
altos[,beds_txt := impute_units(property_name)]
altos[,beds_imp := coalesce(beds, beds_txt)]
altos[,price_per_bed :=as.numeric(price)/beds_imp]
altos[,address_id := .GRP, by = .(pm.house, pm.street, pm.streetSuf, pm.preDir, pm.sufDir,pm.zip)]
altos[,bed_address_id := .GRP, by = .(address_id, beds_imp)]


# merge altos and xwalk
altos_m = merge(altos,
                xwalk_unique,
                by = "n_sn_ss_c",
                all.x = TRUE)

# now begins the process of cleaning the altos data...
# 54% have a unique PID in boston... could be more but condos...
# more like 84% in houston
altos_m[,.N, by = is.na(PID)][,per := N / sum(N)][]
altos_m[,PID_replace := coalesce(PID, address_id)]
altos_m[,bed_bath_pid_ID := .GRP, by = .(PID_replace, beds_imp, baths) ]
altos_m[,beds_round := round(beds_imp,0)]
# choice of whether to windsorize data to get rid of obviously bad listings...
altos_m[,bed_year_percentile := ntile(price, 1000), by = year]
# drop the units listed for < 600 (this is less than 1/1000 of data, so im pretty sure its just typos)
# other units, e.g. super expensive ones can be dropped later

# can sample some non merged to see if there's an obvious match
sample_ids = altos_m[is.na(PID) & !is.na(pm.uid_zip), sample(pm.uid_zip, 1)]

# View(altos_m[address_id %in% sample_ids] )
# View(fa[city_c == "boston" & streetName_c == "beacon"])

# sample some that did merge to check match quality
sample_ids = altos_m[!is.na(PID) & !is.na(pm.uid_zip), sample(pm.uid_zip, 100)]
# View(altos_m[pm.uid_zip %in% sample_ids] %>% distinct(street_address_lower, fa_minus_unit,PID, merge))
# View(fa[PID == as.integer64(2000092171001),.(fullAddress_c, type, landUsage)])
# here are the basic issues:
# it's not clear how long a listing has been up for (e.g, is it still being listed)
# so grabbing the months can be dicey
# it's not clear how much to believe large fluctations in prices
# this is going to be an attempt to smooth this all out

# first step is to make a panel of all listings by ID- ym
altos_m[,num_listings_ym := .N, by = .(bed_bath_pid_ID, ym)]
altos_m[,num_listings_ymw := .N, by = .(bed_bath_pid_ID, ymd)]
altos_m[,num_prices_ym := uniqueN(price), by = .(bed_bath_pid_ID, ym)]
altos_m[,num_prices_ym_round100 := uniqueN(100*round(price/100,0)), by = .(bed_bath_pid_ID, ym)]
altos_m[,num_prices_ymw := uniqueN(price), by = .(bed_bath_pid_ID, ymd)]
altos_m[,num_years_listed := uniqueN(year), by = .(bed_bath_pid_ID)]
altos_m[,num_years_listed_PID := uniqueN(year), by = .(PID_replace)]
altos_m[,listing_id := .GRP, by = .(bed_bath_pid_ID, property_name)]
# try to get some sense of people who just leave their listings on
altos_m = altos_m[order(bed_bath_pid_ID, ymd),]
altos_m[,rep_listing := replace_na(price == lag(price),0), by = listing_id]
altos_m[,time_gap := c(NA, diff(ymd)), by = listing_id]
# have a running variable that resets when a listing changes
altos_m[,cum_rep_listing :=ifelse(rep_listing == 1, seq_len(.N), 0), by = .(listing_id, rleid(rep_listing))]
altos_m[,quantile(cum_rep_listing,na.rm = T, probs = seq(0,1,0.01))]

# sample the listings that have been up for a long time
sample_ids = altos_m[cum_rep_listing > 10 & !is.na(PID) , sample(bed_bath_pid_ID, 1)]
# View(altos_m[bed_bath_pid_ID %in% sample_ids] %>%
#        relocate(bed_bath_pid_ID,PID,property_name,listing_id,
#                 date,street_address_lower,fa_minus_unit, cum_rep_listing,rep_listing,
#                 beds_imp, baths, price, landUsage, ymd) %>%
#        select(-contains("pm.")))

# pretty arbitrary but I'm gonna slice off listings based on whether they've been > 10
# weeks on the market without a price cut
altos_m = altos_m[cum_rep_listing <= 10 ]

# what's going to be true is that a bunch have multiple prices listed within the same
# year-month
ym_listings = altos_m[,.N, by = num_listings_ym][order(num_listings_ym)]
ymw_listings = altos_m[,.N, by = num_listings_ymw][order(num_listings_ymw)]
ym_prices = altos_m[,.N, by = num_prices_ym][order(num_prices_ym)]
ym_prices_round100 = altos_m[,.N, by = num_prices_ym_round100][order(num_prices_ym_round100)]
ymw_prices = altos_m[,.N, by = num_prices_ymw][order(num_prices_ymw)]

ym_listings %>%
  merge(ymw_listings,
        by.x = "num_listings_ym",
        by.y = "num_listings_ymw",
        suffixes = c("_ym_listings","_ymw_listings"),
        all = TRUE) %>%
  merge(ym_prices,
        by.x = "num_listings_ym",
        by.y = "num_prices_ym",
        suffixes = c("_ym_price","_ym_prices"),
        all = TRUE) %>%
  merge(ym_prices_round100,
        by.x = "num_listings_ym",
        by.y = "num_prices_ym_round100",
        suffixes = c("","_ym_prices_round100"),
        all = TRUE) %>%
  merge(ymw_prices,
        by.x = "num_listings_ym",
        by.y = "num_prices_ymw",
        suffixes = c("","_ymw_prices"),
        all = TRUE) %>%
  rename(N_ym_price = N)%>%
  slice(1:15)

unique(altos_m, by ="bed_bath_pid_ID")[,.N, by = num_years_listed][order(num_years_listed)]
unique(altos_m, by ="PID_replace")[,.N, by = num_years_listed_PID][order(num_years_listed_PID)]
print("made vars")

# sample some
sample_ids = altos_m[num_prices_ymw > 5 & !is.na(PID), sample(bed_bath_pid_ID, 1)]
# View(altos_m[bed_bath_pid_ID %in% sample_ids] %>%
#        relocate(bed_bath_pid_ID,PID,property_name,listing_id,
#                 date,street_address_lower,cum_rep_listing,rep_listing,landUsage,
#                 beds_imp, baths, price, landUsage, ymd) %>%
#        select(-contains("pm.")) %>%
#        arrange(bed_bath_pid_ID, ymd, price)
#      )

pid_to_check = as.integer64(2000114413001)

#View(fa[PID == pid_to_check])
altos_m[,price := as.numeric(price)]
# dumb thing but uniqueN is much slower for small T, large N
altos_m[,num_prices := length(unique(price)), by = .(ymd, bed_bath_pid_ID)]
altos_m[,num_prices_year := length(unique(price)), by = .(year, bed_bath_pid_ID)]
altos_m[,num_addresses := length(unique(address_id)), by = .(ymd, bed_bath_pid_ID)]
altos_m[,change_price_year := price != lag(price), by = .(bed_bath_pid_ID, year)]

# now aggregate to unit level...
# cut off the big bed places and big bath places cause they give weird answers...
altos_year_bedrooms = altos_m[beds_imp<=6 & baths <= 4,list(
  med_price = median(price, na.rm = TRUE),
  mean_price = mean(price, na.rm = TRUE),
  sd_price = sd(price, na.rm = TRUE),
  mad_price = mad(price, na.rm = TRUE),
  min_price = min(price, na.rm = TRUE),
  max_price = max(price, na.rm = TRUE),
  num_listings = .N,
  num_prices = uniqueN(price),
  num_price_changes = sum(change_price_year,na.rm = T),
  street_address_lower_first = first(street_address_lower),
  address_id_first = first(address_id),
  beds_imp_first = first(beds_imp),
  baths_first = first(baths),
  #num_addresses = uniqueN(address_id),
  #year = first(year),
  ymd = first(ymd),
  month = first(month),
  day = first(day)
), by = .(PID,bed_bath_pid_ID, year)]

# now aggregate to building level...
# cut off the big bed places and big bath places cause they give weird answers...
altos_year_building = altos_m[beds_imp<=6 & baths <= 4,list(
  med_price = median(price, na.rm = TRUE),
  mean_price = mean(price, na.rm = TRUE),
  sd_price = sd(price, na.rm = TRUE),
  mad_price = mad(price, na.rm = TRUE),
  min_price = min(price, na.rm = TRUE),
  max_price = max(price, na.rm = TRUE),
  num_listings = .N,
  num_prices = uniqueN(price),
  num_price_changes = sum(change_price_year,na.rm = T),
  street_address_lower_first = first(street_address_lower),
  address_id_first = first(address_id),
  #num_addresses = uniqueN(address_id),
  #year = first(year),
  ymd = first(ymd),
  month = first(month),
  day = first(day)
), by = .(PID, year)]



altos_year_building[order(year),year_gap := c(NA, diff(year)), by = PID]
altos_year_bedrooms[order(year),year_gap := c(NA, diff(year)), by = bed_bath_pid_ID]
#
altos_year_building[,num_years_in_sample := uniqueN(year[!is.na(med_price)]), by = PID]
altos_year_building[,max_year_gap := max(year_gap,na.rm = T), by = PID]

altos_year_bedrooms[,num_years_in_sample := uniqueN(year[!is.na(med_price)]), by = bed_bath_pid_ID]
altos_year_bedrooms[,max_year_gap := max(year_gap,na.rm = T), by = bed_bath_pid_ID]

fwrite( altos_year_bedrooms, glue::glue("~/Desktop/data/philly-evict/altos_year_bedrooms_philly.csv"))
fwrite(altos_year_building, glue::glue("~/Desktop/data/philly-evict/altos_year_building_philly.csv"))
