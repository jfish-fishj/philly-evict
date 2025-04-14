library(data.table)
library(tidyverse)
library(sf)
library(tidycensus)
library(fixest)

source('helper-functions.R')

gephilly_lic = fread("/Users/joefish/Desktop/data/philly-evict/business_licenses_clean.csv")
philly_evict = fread("/Users/joefish/Desktop/data/philly-evict/evict_address_cleaned.csv")
philly_parcels = fread("/Users/joefish/Desktop/data/philly-evict/parcel_address_cleaned.csv")
philly_altos = fread("~/Desktop/data/philly-evict/altos_year_bedrooms_philly.csv")
philly_altos_beds = fread("~/Desktop/data/philly-evict/altos_year_bedrooms_philly.csv")
philly_bg = read_sf("~/Desktop/data/philly-evict/philly_bg.shp")
philly_def_names <- fread("~/Desktop/data/philly-evict/phila-lt-data/names_merge_defendants.csv")
philly_plaintiff_names <- fread("~/Desktop/data/philly-evict/phila-lt-data/names_plaintiff_merge.csv")

altos_xwalk = fread("~/Desktop/data/philly-evict/philly_altos_address_agg_xwalk.csv")
evict_xwalk = fread("~/Desktop/data/philly-evict/philly_evict_address_agg_xwalk.csv")
#rgdal::ogrInfo(system.file("/Users/joefish/Desktop/data/philly-evict/opa_properties_public.gdb", package="sf"))

pa_zip <- get_acs(
  geography = "zcta",
  variables = c(
    "meddhinc"=  "B19013_001", # median household income
    "housing_units"=  "B25001_001",
    "renter_occupied"=  "B25003_003",
    "owner_occupied"=  "B25003_002",
    "gross_rent"="B25064_001",
    "contract_rent" = "B25058_001"

  ),
  year = 2015,
  survey = "acs5",
  state = "PA",
  output = "wide"
  #county = "Philadelphia"
)

philly_parcels[,PID := parcel_number]

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

# Comprehensive list of business entity terms (with abbreviations, plurals, and variations)
business_words <- c(
  "LLC", "L\\.L\\.C\\.", "LLCS",
  "LIMITED PARTNERSHIP", "LTD", "L\\.T\\.D\\.", "LTDs",
  "INC", "INC\\.", "INCS", "INCORPORATED",
  "CORP", "CORPORATION", "CORPS",
  "L\\.P\\.", "LP", "LPS",
  "LLP", "LLPS",
  "CO", "CO\\.", "COMPANY", "COMPANIES",
  "HOLDING", "HOLDINGS",
  "PARTNERSHIP", "PARTNERSHIPS", "PARTNER", "PARTNERS",
  "ASSOC", "ASSOCS", "ASSOCIATES", "ASSOCIATION", "ASSOCIATIONS",
  "ENTERPRISE", "ENTERPRISES",
  "VENTURE", "VENTURES",
  "GROUP", "GROUPS",
  "SOLUTIONS", "STRATEGIES",
  "BROS", "BROTHERS",
  "FIRM", "FIRMS",
  "TRUST", "TRUSTS",
  "HOUS","HOUSING","APARTMENTS","APTS?","REAL",
  "ESTATE","REALTY","MANAGEMENT","MGMT",
  "DEV","DEVELOPMENT","DEVELOPS","DEVELOPERS",
  "THE"
)

# Standardization function
# Create regex pattern (case insensitive)
pattern <- str_c("\\b(", str_c(business_words, collapse = "|"), ")\\b", collapse = "|")


philly_evict[,commercial_alt := str_detect(str_to_upper(defendant),pattern)]

philly_evict_m = merge(
  philly_evict,
  evict_xwalk[num_parcels_matched == 1,.(PID, n_sn_ss_c)], by = "n_sn_ss_c"
)

philly_evict_m[,housing_auth := str_detect(str_to_upper(plaintiff), "PHILA.+A?UTH|HOUS.+AUTH")]

# create parcel-year data for eviction
philly_evict_agg = philly_evict_m[ commercial_alt==F,list(
  num_filings = sum(housing_auth==F ),
  num_filings_with_houth_auth = .N,
  med_price = median(ongoing_rent[housing_auth==F ]),
  pm.zip = first(pm.zip),
  ymd = as.character(first((d_filing)))
), by = .(year, PID)][,source := "evict"]

philly_evict_agg_zip = philly_evict_agg[,list(
  num_filings_zip = sum(num_filings_with_houth_auth,na.rm=T),
  med_price_zip = median(med_price,na.rm=T)
), by = .(year, pm.zip)][,source := "evict"]

philly_evict_agg_zip_m = merge(
  philly_evict_agg_zip,
  pa_zip %>% mutate(pm.zip = GEOID) %>%
    select(pm.zip, matches("E$")),
  by = "pm.zip"
) %>%
  merge(zipcodeR::zip_code_db %>% select(zipcode, major_city), by.x = "pm.zip", by.y = "zipcode")
philly_evict_agg_zip_m = philly_evict_agg_zip_m[major_city=="Philadelphia" & num_filings_zip > 10]
philly_evict_agg_zip_m[,filing_rate_zip := num_filings_zip/renter_occupiedE]
philly_evict_agg_zip_m[,spatstat.geom::weighted.quantile(filing_rate_zip, renter_occupiedE, na.rm =T, probs = seq(0,1,0.1))] %>%
  round(3)
philly_evict_agg[,num_filings_total := sum(num_filings), by = PID]
# merge with altos data and evict data
rent_df = bind_rows(
  philly_altos[PID %in% philly_rentals_long$PID] %>%
    mutate(ymd = paste0(year, "-", month, "-", day),source = "altos") %>%
    select(PID, bed_bath_pid_ID,beds_imp_first,baths_first, ymd,year, med_price,num_listings, source),
  philly_evict_agg[PID %in% philly_rentals_long$PID] %>%
    select(PID, year, med_price, ymd,num_filings,num_filings_total, source)
)

rent_df_f = rent_df[year %in% 2006:2019 & med_price > 500 & med_price < 7500]

philly_parcels_first = philly_parcels[,.SD[1], by = "PID"]

# merge spatial data onto philly_parcels_first
philly_parcels_first_sf = st_as_sf(philly_parcels_first, coords = c("geocode_x", "geocode_y"), crs = st_crs(philly_bg)) %>%
  st_join(philly_bg)

philly_parcels_first_sf = philly_parcels_first_sf %>%
  select(-geometry) %>%
  as.data.table()
philly_rentals_long_old = copy(philly_rentals_long)
philly_rentals_long = philly_rentals_long[!is.na(PID),list(
  numberofunits = sum(numberofunits),pm.zip = first(pm.zip)
), by = .(year, PID)]
philly_rentals_long[,min_units := min(numberofunits), by = .(PID)]
philly_rentals_long[,max_units := max(numberofunits), by = .(PID)]
philly_rentals_long[,spread_units := max_units - min_units]

philly_rentals_long_year = philly_rentals_long[year <= 2019,list(num_units = median(numberofunits)),
                                               by = .(PID = as.integer(PID))]

philly_parcels_first_sf[,owner := paste(owner_1, owner_2)]
philly_parcels_first_sf[,owner_mailing := paste(owner, mailing_street)]

# now merge parcel chars
rent_df_f_parcels = merge(
  rent_df_f,
  philly_parcels_first_sf %>% select(
    basements,location,
    building_code_description:geographic_ward,market_value,taxable_building,taxable_land,
    number_of_bathrooms:other_building,quality_grade,topography,
    separate_utilities:state_code, total_area:year_built_estimate,
    building_code_new,building_code_description_new, zoning, zip_code,owner_mailing,
     PID, GEOID, CT_ID_10
  ), by = "PID"
) %>%
  merge(philly_rentals_long_year, by = c("PID")) %>%
  merge(philly_evict_agg_zip_m %>%mutate(pm.zip = as.numeric(pm.zip)) %>%select(-c(NAME, source)),
        by.x = c("zip_code", "year"),by.y = c("pm.zip",'year'), all.x = T)

rent_df_f_parcels[,num_filings := ifelse(is.na(num_filings),0,num_filings)]
rent_df_f_parcels[,num_filings_total := ifelse(is.na(num_filings_total),0,num_filings_total)]
rent_df_f_parcels[,multi_source := uniqueN(source), by = .(PID, year)]
rent_df_f_parcels[,num_filings_total_source := max(num_filings_total), by = .(PID)]
rent_df_f_parcels[,num_filings_source := max(num_filings), by = .(PID,year)]
rent_df_f_parcels[,filing_rate := num_filings_total_source/num_units/uniqueN(year)]
# clean some vars
# first replace missing vals w/ NA
for (var in c("quality_grade","building_code_description","building_code_description_new",

              "basements","number_of_bathrooms","separate_utilities","total_area",
              "building_code_new","zoning","state_code","zip_code") )  {
  rent_df_f_parcels[get(var) == "",(var) := NA_character_]
}

# now fix invalud vals
rent_df_f_parcels[,quality_grade_fixed := fifelse(quality_grade %in% c(
  "A","A+", "A-", "B", "B+", "B-", "C", "C+", "C-", "D", "D+", "D-", "F"
), quality_grade, NA_character_)]

# building_code_description_new     N
# <char> <int>
#   1:               ROW PORCH FRONT 43439
# 2:                   ROW TYPICAL 38436
# 3:                  ROW POST WAR 16587
# 4:         APARTMENTS - LOW RISE 16096
# 5:             TWIN CONVENTIONAL  8959
# 6:     APARTMENTS - BLT AS RESID  7698
# 7:                    ROW MODERN  6417
# 8:                          <NA>  5815
# 9:  ROW MIXED-COM/RES-BLT AS COM  5339
# 10:                TWIN OLD STYLE  4922
# 11:  ROW MIXED-COM/RES-BLT AS RES  4555
# 12:                 TWIN POST WAR  3728
# 13:              APTS - HIGH RISE  3043
# 14:         APARTMENTS - MID RISE  2881
# 15:                 ROW OLD STYLE  1945
# 16:                 ROW RIVER ROW  1945
# 17:                 APTS - GARDEN  1300
# 18:          CONDO NON CONFORMING  1080
# 19:                    TWIN RANCH   667
# 20:                     OLD STYLE   636

rent_df_f_parcels[,building_code_description_new_fixed := case_when(
  str_detect(building_code_description_new, "ROW") ~ "ROW",
  str_detect(building_code_description_new, "TWIN") ~ "TWIN",
  str_detect(building_code_description_new, "APARTMENTS|APTS") & str_detect(building_code_description_new,"LOW")~ "LOW RISE APARTMENTS",
  str_detect(building_code_description_new, "APARTMENTS|APTS") & str_detect(building_code_description_new,"MID")~ "MID RISE APARTMENTS",
  str_detect(building_code_description_new, "APARTMENTS|APTS") & str_detect(building_code_description_new,"HIGH")~ "HIGH RISE APARTMENTS",
  str_detect(building_code_description_new, "APARTMENTS|APTS") & str_detect(building_code_description_new,"GARDEN")~ "GARDEN APARTMENTS",
  str_detect(building_code_description_new, "APARTMENTS|APTS") ~ "APARTMENTS OTHER",
  str_detect(building_code_description_new, "CONDO") ~ "CONDO",
  str_detect(building_code_description_new, "OLD") ~ "OLD STYLE",
  is.na(building_code_description_new) ~ NA_character_,
  TRUE ~ "OTHER"
)]

rent_df_f_parcels[,beds_imp_first_fixed := fifelse(
  !is.na(beds_imp_first), as.character(round(beds_imp_first)), "UNKNOWN"
) ]

rent_df_f_parcels[,baths_first_fixed := fifelse(
  !is.na(baths_first), as.character(round(baths_first)), "UNKNOWN"
) ]



rent_df_f_parcels[,log_med_price := log(med_price)]
analytic_df = (rent_df_f_parcels[
  !is.na(year_built_estimate)
  & !is.na(total_area)
  & !is.na(num_units)
  & !is.na(log_med_price)
  & !is.na(num_filings)
  & !is.na(log(num_filings))
  & !is.na(log(num_units))
  & !is.na(log(total_area))
  & !is.na(quality_grade_fixed)
  & !is.na(zoning)
  & !is.na(building_code_description_new_fixed)
  & total_area < 1e5
  &!is.na(GEOID)
  & num_units > 0
  &!is.na(number_stories)
  & total_area > 1000
  #& filing_rate <= 3
])
analytic_df[,month := lubridate::month(ymd)]
analytic_df[,filing_rate_sq := filing_rate^2]
analytic_df[,filing_rate_cube := filing_rate^3]
analytic_df[,num_filings_total_sq := num_filings_total^2]


philly_rentals_long[,PID := as.numeric(PID)]
share_df = unique(philly_rentals_long[year <= 2019 & !is.na(PID)], by =c("PID","year")) %>%
  merge(philly_parcels_first_sf[,.(PID, owner_mailing)], by = "PID", all.x = T) %>%
  merge(unique(rent_df_f_parcels[,.(PID, year, filing_rate, num_filings_total_source)],
               by =c("PID","year")),
        by = c("PID","year"), all.x = T )
share_df[is.na(owner_mailing),owner_mailing := sample(1:sum(is.na(owner_mailing)), replace = F)]
share_df[,num_units := (numberofunits)]
share_df[,num_units_bins := case_when(
  num_units == 1 ~ "1",
  num_units <= 5 ~ "2-5",
  num_units <= 50 ~ "6-60",
  num_units > 50 ~ "51+",
 TRUE ~ NA_character_
)]
share_df[,filing_rate := replace_na(filing_rate,0)]
share_df[,num_units_owner := sum(num_units), by = .(year,pm.zip, owner_mailing)]
share_df[,num_units_zip := sum(num_units), by = .(year,pm.zip)]
share_df[,share_units_zip := num_units_owner/num_units_zip]

share_df[,num_units_evict := sum(num_units[ filing_rate > 0.15 ],na.rm=T), by = .(pm.zip,year)]
share_df[,num_units_evict_owner :=  sum(num_units[filing_rate > 0.15],na.rm=T), by = .(year,pm.zip, owner_mailing)]
share_df[,share_units_evict := fifelse(filing_rate > 0.15 , num_units_evict_owner / num_units_evict, 0) ]

share_df[,num_units_evict_unit_bins := sum(num_units[filing_rate > 0.15],na.rm=T), by = .(year,pm.zip, num_units_bins)]
share_df[,num_units_evict_owner_bins := sum(num_units[filing_rate > 0.15],na.rm=T), by = .(year,pm.zip, owner_mailing, num_units_bins)]
share_df[,share_units_evict_bins := fifelse(filing_rate > 0.15 , num_units_evict_owner_bins / num_units_evict_unit_bins, 0) ]

share_df[,num_units_unit_bins := sum(num_units), by = .(year,pm.zip, num_units_bins)]
share_df[,num_units_owner_bins := sum(num_units), by = .(year,pm.zip, owner_mailing, num_units_bins)]
share_df[,share_units_bins := num_units_owner_bins/num_units_unit_bins]

hhi_df = unique(share_df, by = c('year','pm.zip', 'owner_mailing'))
hhi_df[,hhi := sum((100*share_units_zip)^2),
       by = .(year,pm.zip)]
hhi_df[,hhi_evict := sum((100*share_units_evict)^2),
         by = .(year,pm.zip)]

hhi_df_units = unique(share_df, by = c('year','pm.zip','owner_mailing', 'num_units_bins'))
hhi_df_units[,hhi := sum((100*share_units_bins)^2),
       by = .(year,pm.zip,num_units_bins)]

hhi_df_units[,hhi_evict := sum((100*share_units_evict_bins)^2),
         by = .(year,pm.zip,num_units_bins)]

share_df = merge(
  share_df %>% select(-contains('hhi')), unique(hhi_df, by = c('year','pm.zip'))[,.(year,pm.zip,hhi, hhi_evict)],
  by = c('year','pm.zip')
)

share_df = merge(
  share_df, unique(hhi_df_units, by = c('year','pm.zip','owner_mailing'))[,.(year,pm.zip,hhi,owner_mailing, hhi_evict)],
  by = c('year','pm.zip','owner_mailing'), suffixes = c("","_unit_bins")
)


share_df[hhi_evict>0,weighted.mean(hhi_evict, num_units)]
share_df[,weighted.mean(hhi, num_units)]
share_df[,weighted.mean(hhi_unit_bins, num_units)]
share_df[hhi_evict_unit_bins>0,weighted.mean(hhi_evict_unit_bins, num_units)]

share_df[order(share_units_evict), cum_percent := cumsum(share_units_evict*num_units[filing_rate > 0.15])/sum(share_units_evict[num_units[filing_rate > 0.15]]), by = .(year,pm.zip)]

share_df[share_units_evict >0,spatstat.geom::weighted.quantile(share_units_evict, num_units, na.rm =T, probs = seq(0,1,0.1))] %>%
  round(3)

share_df[share_units_evict >0,spatstat.geom::weighted.quantile(share_units_evict_bins, num_units, na.rm =T, probs = seq(0,1,0.1))] %>%
  round(3)

share_df[,spatstat.geom::weighted.quantile(share_units_zip, num_units, na.rm =T, probs = seq(0,1,0.1))] %>%
  round(3)
share_df[,spatstat.geom::weighted.quantile(hhi, num_units, na.rm =T, probs = seq(0,1,0.1))]
share_df[,spatstat.geom::weighted.quantile(hhi_evict, num_units, na.rm =T, probs = seq(0,1,0.1))] %>%
  round(3)

share_df[,spatstat.geom::weighted.quantile(hhi_evict_unit_bins, num_units, na.rm =T, probs = seq(0,1,0.1))] %>%
  round(3)

market_shares <- ggplot(share_df, aes(weight = num_units)) +
  stat_ecdf(aes( x= (share_units_zip),
                 color = 'Zip Code')) +
  stat_ecdf(data = share_df[share_units_evict > 0], aes( x= (share_units_evict),
                                                         color = 'Zip Code X High-Evicting Units')) +
  stat_ecdf(aes( x= (share_units_bins),
                 color = 'Zip Code X Property Size'))+
  stat_ecdf(data = share_df[share_units_evict_bins > 0],aes( x= (share_units_evict_bins),
                                                             color = 'Zip Code X High-Evicting Units X Property Size')) +
  labs(
    x = 'Share of Units',
    y = 'Cumulative Density',
    title = 'Cumulative Distribution of Market Shares',
    subtitle = "Weighted by Number of Units",
    color = 'Market Definition'
  ) +
  scale_x_continuous(breaks = seq(0,1,0.1))+
  scale_y_continuous(breaks = seq(0,1,0.1)) +
  theme_philly_evict()

ggsave(market_shares, file = "figs/market_share_cdf.png",
       width = 10, height = 6, dpi = 300)

share_df_agg = share_df[,list(
  share_units_zip = mean(share_units_zip,na.rm=T),
  share_units_evict = mean(share_units_evict,na.rm=T),
  share_units_bins = mean(share_units_bins,na.rm=T),
  share_units_evict_bins = mean(share_units_evict_bins,na.rm=T),
  num_units_zip = mean(num_units_zip,na.rm=T),
  num_units_evict = mean(num_units_evict,na.rm=T),
  num_unit_bins = first(num_units_bins),
  hhi = mean(hhi,na.rm=T),
  hhi_evict = mean(hhi_evict,na.rm=T),
  hhi_evict_unit_bins = mean(hhi_evict_unit_bins,na.rm=T),
  hhi_unit_bins = mean(hhi_unit_bins,na.rm=T)
), by = .(PID = as.numeric(PID))]

share_df_agg[,.N, by = is.na(share_units_evict)]


analytic_df = merge(analytic_df%>% select(-matches("share_units|num_units?_|hhi")), share_df_agg[],
                    by = c("PID"))

filings_resid_reg <- fixest::feols(filing_rate ~ poly(num_units,2) + poly(year_built,2)+poly(number_stories,2)+multi_source|
                                     GEOID^year+type_heater +quality_grade_fixed +
                                     view_type + building_code_description_new_fixed+
                                     general_construction , data = analytic_df)

analytic_df[,filings_resid := filings_resid_reg$residuals ]

# predict beds
bed_reg <- fixest::feols(beds_imp_first ~ poly(med_price,2)*building_code_description_new_fixed + poly(num_units,2) + poly(year,2)
                         + poly(year_built,2)+poly(number_stories,2)+multi_source|
                          GEOID+type_heater +quality_grade_fixed +
                          view_type +
                          general_construction ,combine.quick = F,
                         data = analytic_df)

analytic_df[,beds_imp_first_pred := predict(bed_reg, newdata = analytic_df) %>% round()]
analytic_df[,beds_imp_first_pred := case_when(
  beds_imp_first_pred < 0 ~ 0,
  beds_imp_first_pred > 5 ~ 5,
  TRUE ~ beds_imp_first_pred
)]
# predict baths
bath_reg <- fixest::feols(baths_first ~ poly(med_price,2)*building_code_description_new_fixed + poly(num_units,2) + poly(year,2)
                          + poly(year_built,2)+poly(number_stories,2)+multi_source|
                           GEOID+type_heater +quality_grade_fixed +
                           view_type +
                           general_construction ,combine.quick = F,
                          data = analytic_df)

analytic_df[,baths_first_pred := predict(bath_reg, newdata = analytic_df) %>% round()]
analytic_df[,baths_first_pred := case_when(
  baths_first_pred < 1 ~ 1,
  baths_first_pred > 4 ~ 4,
  TRUE ~ baths_first_pred
)]

analytic_df[,share_units_evict_sq := share_units_evict^2]
analytic_df[,share_units_zip_sq := share_units_zip^2]

analytic_df[,num_rentals := uniqueN(PID), by = .(GEOID,year)]
analytic_df[,filing_rate_g25 := fifelse(filing_rate > 0.25,1,0)]
analytic_df[,filing_rate_g50 := fifelse(filing_rate > 0.5,1,0)]
analytic_df[,filing_rate_zero := fifelse(filing_rate == 0,1,0)]
# idk run some hedonics


m0 <- fixest::feols(
  log_med_price ~filing_rate| year,
  data = analytic_df[ source == "evict"& filing_rate < 1 & !is.na(baths_first_pred) ],
  weights = ~num_units,
  cluster = ~PID
)


summary(m0)


m1 <- fixest::feols(
  log_med_price ~ filing_rate   +poly((num_units),3)
  + poly(year_built,3)+poly(number_stories,3)
   #+ poly(beds_imp_first_pred,3) + poly(baths_first_pred,3)
  |GEOID^year+quality_grade_fixed+
    building_code_description_new_fixed+
    beds_imp_first_pred+baths_first_pred
    #general_construction ,
 , data = analytic_df[source == "evict"& filing_rate < 2 &!is.na(baths_first_pred) ],
  #weights = ~(num_units),
  cluster = ~PID,
  combine.quick = F
)
summary(m1)
analytic_df[,resids := log_med_price - predict(m1, newdata = analytic_df)]
#View(analytic_df[order(desc(abs(resids)))][1:100])

quantile(m1$residuals)
summary(m1)





m2 = fixest::feols(
  log_med_price ~ filing_rate + filing_rate_sq + poly(num_units,2) + poly(year_built,2)+poly(number_stories,2) +
    poly(beds_imp_first_pred,2) + poly(baths_first_pred,2)|
    GEOID^year+type_heater +quality_grade_fixed +
    view_type + building_code_description_new_fixed+
    general_construction ,
  data = analytic_df[source == "evict" & filing_rate < 1 &   !is.na(baths_first_pred)],
  #weights = ~num_units,
  cluster = ~PID,
  combine.quick = F
)

# same thing but full sample
m3 = fixest::feols(
  log_med_price ~filing_rate| year,
  data = analytic_df[ filing_rate < 1 & !is.na(baths_first_pred) ],
  #weights = ~num_units,
  cluster = ~PID
)

m4 = fixest::feols(
  log_med_price ~ filing_rate  + poly(log(num_units),3) + poly(year_built,3)+poly(number_stories,3) +
    poly(beds_imp_first_pred,2) + poly(baths_first_pred,2)|
    GEOID^year +quality_grade_fixed +
     building_code_description_new_fixed+
    general_construction ,
  data = analytic_df[filing_rate < 1& !is.na(baths_first_pred)],
  #weights = ~num_units,
  cluster = ~PID,
  combine.quick = F
)

m5 = fixest::feols(
  log_med_price ~ filing_rate*source  +num_unit_bins+ poly(num_units,2) + poly(year_built,2)+poly(number_stories,2) +
    poly(beds_imp_first_pred,2) + poly(baths_first_pred,2)|
    GEOID^year+type_heater +quality_grade_fixed +
    view_type + building_code_description_new_fixed+
    general_construction ,
  data = analytic_df[filing_rate>0 & filing_rate < 1& !is.na(baths_first_pred)],
  #weights = ~num_units,
  cluster = ~PID,
  combine.quick = F
)

# export tables:
# make table
setFixest_dict(
  c(
    "source"= "Data Source",
    "sourceevict"= "Eviction Rental",
    "year" = "Year",
    "log_med_price" = "Log(Price)",
    "bed_bath_pid_ID" = "Unit",
    "ATT" = "ATT",
    "num_listings" = "Listings",
    "num_units" = "Number of Units",
    "year_built" = "Year Built",
    "number_stories" = "Number of Stories",
    "quality_grade_fixed" = "Quality Grade",
    "building_code_description_new_fixed" = "Building Code",
    "zoning" = "Zoning",
    "zip_code" = "Zip Code",
    "PID" = "Parcel ID",
    "GEOID" = "Census Block Group",
    "CT_ID_10" = "Census Tract",
    "num_filings" = "Number of Filings",
    "multi_source" = "Multiple Sources",
    "num_filings_total_source" = "Total Filings",
    "num_filings_source" = "Filings",
    "filing_rate" = "Filing Rate",
    "filing_rate_sq" = "Filing Rate^2",
    "filing_rate_cube" = "Filing Rate^3",
    "num_filings_total_sq" = "Total Filings^2",
    "share_units_zip" = "Share of Units in Zip",
    "share_units_evict" = "Share of Units in Zip with Eviction",
    "share_units_evict_sq" = "Share of Units in Zip with Eviction^2",
    "num_units_zip" = "Number of Units in Zip",
    "num_units_evict" = "Number of Units with Eviction",
    "num_rentals" = "Number of Rentals",
    "beds_imp_first_pred" = "Beds (Imputed)",
    "baths_first_pred" = "Baths (Imputed)",
    'view_type' = "View Type",
    'type_heater' = "Heater Type",
    'general_construction'="Construction Type"
  )
)
# set default style
def_style = style.df(depvar.title = "", fixef.title = "",
                     fixef.suffix = " fixed effect", yesNo = c("Yes","No"))

fixest::setFixest_etable(
  digits = 4, fitstat = c("n","r2")
)

model_tables_m0_m1 = etable(
  m0, m1,
  title = "Rent Price Regressions",
  #caption = "Philadelphia, 2006-2019",
  keep = "Eviction|Filing",
  order = "Filing Rate",
  # add lines for extra controls
  extralines = list(
    "Polynomial Imputed Beds" = c("No", "Yes"),
    "Polynomial Imputed Baths" = c("No", "Yes"),
    "Polynomial Year Built" = c("No", "Yes"),
    "Polynomial Number of Stories" = c("No", "Yes"),
    "Polynomial Number of Units" = c("No", "Yes")

  ),
  label = "tab:rent_regs",
  tex = TRUE
)
writeLines(model_tables_m0_m1, "model_tables.tex")

# export all models
model_tables_all_models = etable(
  m0, m1, m3, m4, m5,
  title = "Rent Price Regressions",
  #caption = "Philadelphia, 2006-2019",
  keep = "Eviction|Filing",
  order = "Filing Rate",
  # add lines for extra controls
  extralines = list(
    "Polynomial Imputed Beds" = c("No", "Yes","No","Yes","Yes"),
    "Polynomial Imputed Baths" = c("No", "Yes","No","Yes","Yes"),
    "Polynomial Year Built" = c("No", "Yes","No","Yes","Yes"),
    "Polynomial Number of Stories" = c("No", "Yes","No","Yes","Yes"),
    "Polynomial Number of Units" = c("No", "Yes","No","Yes","Yes")

  ),
  label = "tab:rent_regs",
  tex = TRUE
)

writeLines(model_tables_all_models, "model_tables_all_models.tex")

# now do share regs
m0_share <- fixest::feols(
  log_med_price ~ share_units_evict + share_units_evict_sq | year,
  data = analytic_df[share_units_evict>0 & filing_rate < 1 & !is.na(baths_first_pred) & !is.na(share_units_evict)],
  cluster = ~PID
)

summary(m0_share)

m1_share <- fixest::feols(
  log_med_price ~ share_units_evict_bins +share_units_evict_bins^2+
    i(num_unit_bins, ref = "1") +
    poly(log(num_units_evict),3)+
    poly(log(num_units),3)+
    poly(number_stories,3) +
    poly(year_built,3)
    #poly(beds_imp_first_pred,2) + poly(baths_first_pred,2)|
    |year^CT_ID_10 +quality_grade_fixed+ building_code_description_new_fixed+beds_imp_first_pred+baths_first_pred ,
 , data = analytic_df[ share_units_evict>0 & filing_rate < 1 &!is.na(baths_first_pred) & !is.na(share_units_evict)],
  #weights = ~num_units,
  cluster = ~PID,
  combine.quick = F
)

summary(m1_share)

m1_share_alt <- fixest::feols(
  log_med_price ~ share_units_evict +# share_units_zip_sq+
    i(num_unit_bins, ref = "1") +
    log(num_units_zip)+
    poly(log(num_units),2)+
    poly(number_stories,2) +
    poly(year_built,2)+
    poly(beds_imp_first_pred,2) + poly(baths_first_pred,2)|
    GEOID^year+source +quality_grade_fixed+source ,
  , data = analytic_df[  filing_rate < 1 &!is.na(baths_first_pred) & !is.na(share_units_evict)],
  #weights = ~num_units,
  cluster = ~PID,
  combine.quick = F
)

summary(m1_share_alt)


m3_share = fixest::feols(
  log_med_price ~ share_units_zip +share_units_zip_sq| year,
  data = analytic_df[ filing_rate < 1 & !is.na(baths_first_pred) ],
  #weights = ~num_units,
  cluster = ~PID
)

m4_share = fixest::feols(
  log_med_price ~ share_units_zip +share_units_zip_sq  + poly(year_built,2)+poly(number_stories,2) +
    poly(beds_imp_first_pred,2) + poly(baths_first_pred,2) +num_unit_bins|
    GEOID^year+type_heater +quality_grade_fixed +
    view_type + source+
    general_construction ,
  data = analytic_df[filing_rate < 1& !is.na(baths_first_pred)],
  #weights = ~num_units,
  cluster = ~PID,
  combine.quick = F
)


# same thing but residualized plots
# first unconditional prices vs filings
# residualize out year fe
year_resid_filing_reg<- fixest::feols(
  filing_rate ~ 1|year,
  data = analytic_df[ filing_rate < 1],
  # weights = ~num_units,
  cluster = ~PID
)

year_resid_price_reg<- fixest::feols(
  log_med_price ~ 1|year,
  data = analytic_df[ filing_rate < 1],
  # weights = ~num_units,
  cluster = ~PID
)

analytic_df[,filings_year_resid := filing_rate - predict(year_resid_filing_reg, newdata = analytic_df) ]
analytic_df[,prices_year_resid := log_med_price - predict(year_resid_price_reg, newdata = analytic_df) ]

ggplot(analytic_df[year %in% 2006:2019 & filing_rate < 1  ],
       aes(y = prices_year_resid, x = filings_year_resid)) +
  geom_point() +
  geom_smooth() +
  labs(
    title = "Residualized log median rent vs Residualized filings",
    subtitle = "Philadelphia, 2006-2019",
    y = "log median rent",
    x = "residualized filings"
  ) +
  theme_philly_evict() +
  facet_wrap(~source)

# same thing but residualize all property controls
prop_resid_reg<- fixest::feols(
  filing_rate ~ poly(num_units,2) + poly(year_built,2)+poly(number_stories,2) +
    poly(beds_imp_first_pred,2) + poly(baths_first_pred,2)|
    GEOID^year+type_heater +quality_grade_fixed +
    view_type + building_code_description_new_fixed+
    general_construction + source,
  data = analytic_df[!is.na(beds_imp_first_pred)& filing_rate < 1],
  combine.quick = F,
  # weights = ~num_units,
  cluster = ~PID
)

price_resid_reg<- fixest::feols(
  log_med_price ~ poly(num_units,2) + poly(year_built,2)+poly(number_stories,2) +
    poly(beds_imp_first_pred,2) + poly(baths_first_pred,2)|
    GEOID^year+type_heater +quality_grade_fixed +
    view_type + building_code_description_new_fixed+
    general_construction + source,
  data = analytic_df[!is.na(beds_imp_first_pred) & filing_rate < 1],
  combine.quick = F,
  # weights = ~num_units,
  cluster = ~PID
)

analytic_df[,filings_prop_resid := filing_rate - predict(prop_resid_reg, newdata = analytic_df) ]
analytic_df[,prices_prop_resid := log_med_price - predict(price_resid_reg, newdata = analytic_df) ]

ggplot(analytic_df[year %in% 2006:2019 & filing_rate < 1 & abs(prices_prop_resid)<1   ],
       aes(y = prices_prop_resid, x = filings_prop_resid)) +
  geom_point() +
  geom_smooth() +
  labs(
    title = "Residualized log median rent vs filings",
    subtitle = "Philadelphia, 2006-2019",
    y = "log median rent",
    x = "residualized filings"
  ) +
  theme_minimal() +
  facet_wrap(~source)


fwlplot::fwlplot(
  log_med_price ~
    filing_rate_sq+filing_rate +source + log(num_units) + poly(year_built,2) |
    GEOID+year +quality_grade_fixed+building_code_description_new_fixed + zoning ,
  data = analytic_df[filing_rate<=2 & num_filings_total_source < 1000 ]
)

fwlplot::fwlplot(
  log_med_price ~
    filing_rate|year
  ,data = analytic_df[filing_rate<=2 & num_filings_total_source < 1000 ]
)

pred_df =
  expand_grid(
    share_units_evict = seq(0,1,0.1),
    source = c("evict","altos")
  ) %>%
  group_by(source)%>%
  mutate(
    filing_rate = 1,
    num_filings_total = seq(0,100,10),
  filing_rate_sq = filing_rate^2,
  filing_rate_cube = filing_rate^3,
  num_filings_total_sq = num_filings_total^2,
  num_units = mean(analytic_df$num_units),
  year_built = mean(analytic_df$year_built),
  number_stories = mean(analytic_df$number_stories),
  quality_grade_fixed = first(analytic_df$quality_grade_fixed),
  building_code_description_new_fixed = first(analytic_df$building_code_description_new_fixed),
  zoning = first(analytic_df$zoning),
  #source = "altos",
  type_heater = first(analytic_df$type_heater),
  GEOID = first(analytic_df$GEOID),
  multi_source = first(analytic_df$multi_source),
  year = 2019,
  exterior_condition= first(analytic_df$exterior_condition),
  general_construction = first(analytic_df$general_construction),
  view_type = first(analytic_df$view_type),
  beds_imp_first_pred = first(analytic_df$beds_imp_first_pred),
  baths_first_pred = first(analytic_df$baths_first_pred)


)

pred_df = pred_df %>%
  ungroup()%>%
  #group_by(source)%>%
  mutate(
    preds = predict(m1, newdata = pred_df),
    preds_se = broom::tidy(m1) %>% filter(term == "filing_rate") %>% pull(std.error),
    preds_m0 = predict(m0, newdata = pred_df),
    preds_m0_se = broom::tidy(m0) %>% filter(term == "filing_rate") %>% pull(std.error),
    preds_high = preds + 1.96*preds_se,
    preds_low = preds - 1.96*preds_se,
    preds_m0_high = preds_m0 + 1.96*preds_m0_se,
    preds_m0_low = preds_m0 - 1.96*preds_m0_se
    #se = (predict(m1, newdata = pred_df, type = "response")$se.fit)
  )

setDT(pred_df)
pred_df[,preds_norm := preds - preds[share_units_evict == 0], by  = source]
pred_df[,preds_norm_high := preds_norm + 1.96*preds_se, by  = source]
pred_df[,preds_norm_low := preds_norm - 1.96*preds_se, by  = source]

pred_df[,preds_norm_m0 := preds_m0 - preds_m0[share_units_evict == 0], by  = source]
pred_df[,preds_norm_m0_high := preds_norm_m0 + 1.96*preds_m0_se, by  = source]
pred_df[,preds_norm_m0_low := preds_norm_m0 - 1.96*preds_m0_se, by  = source]

ggplot(pred_df[], aes(x = share_units_evict, y = preds_norm,group = source, color = source)) +
  geom_line(aes(linetype = "m1")) +
  geom_line(aes( y = preds_norm_m0,group = source, color = source, linetype = "m0")) +
  #geom_ribbon(aes(ymin = preds_norm_low, ymax = preds_norm_high)) +
  #geom_ribbon(aes(ymin = preds_norm_m0_low, ymax = preds_norm_m0_high)) +
  #geom_smooth(method = "lm") +
  geom_hline(yintercept = 0, linetype = "dashed") +
  theme_minimal()

ggplot(analytic_df[year %in% c(2018,2019)], aes( x= med_price, group = source, color = source)) +
  geom_density() +
  labs(
    title = "Density of median rents by data source",
    subtitle = "Philadelphia, 2018-2019",
    footnote = "Data from Eviction Lab and Altos Research",
    x = "median rents are windsorized at 500 and 4000 for aesthetics"
  ) +
  scale_x_continuous(breaks = seq(500, 4000,500), limits = c(500, 4000))+
  # make the x axis exponentiated
  # scale_x_continuous(trans = "log",
  #                    breaks =round(exp(seq(6,9,0.25))/100) * 100) +
  # geom_vline(aes(xintercept = analytic_df[year %in% c(2018,2019) & source == "evict",
  #                                         median(med_price, na.rm = T)],
  #                color = "evict"
  #                ),
  #            ) +
  # geom_vline(aes(xintercept = analytic_df[year %in% c(2018,2019) & source == "altos",
  #                                         median(med_price, na.rm = T)],
  #                color = "altos"
  # ),
  # ) +
  theme_philly_evict()

ggsave(
  filename = "figs/density_rent_prices.png",
  width = 10,
  height = 6,
  dpi = 300,
  bg = "white"
)

ggplot(analytic_df[year %in% c(2018,2019)], aes( x= med_price, group = source, color = source)) +
  stat_ecdf() +
  labs(
    title = "CDF of rent prices by data source",
    subtitle = "Philadelphia, 2018-2019",
    x = "log median rent"
  ) +
  scale_x_continuous(breaks = seq(500, 4000,500), limits = c(500,4000))+
  theme_philly_evict()


fwlplot::fwl_plot(
  log_med_price ~ share_units_evict + share_units_evict_sq |
    year^GEOID+source  + quality_grade_fixed
  , data = analytic_df[
    num_units > 20 &
    source == "evict"
    # filing_rate <1 & filing_rate >0.2 & share_units_evict >0 &
    #   share_units_evict < 1 & !is.na(baths_first_pred) & !is.na(share_units_evict)
    ]
)

