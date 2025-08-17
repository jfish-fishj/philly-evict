library(data.table)
library(tidyverse)
library(glue)
library(fixest)
source("r/helper-functions.R")
#### files ####
corelogic_input_dir = '~/Desktop/data/corelogic/cleaned-addresses/'
altos_input_dir = "~/Desktop/data/altos/cities/"
xwalk_dir = "~/Desktop/data/altos-corelogic-xwalk/"
outdir = '/Users/joefish/Desktop/data/altos-corelogic-agg/'

for(row in 1:nrow(cities_fips)){
  metro = cities_fips$city[row]
  fips_code = cities_fips$fips[row]
  print(glue::glue("Processing {metro} and ({fips_code})"))
  output_file = glue("{outdir}/{metro}_{fips_code}.csv")
  # check if output file exists and if it does skip
  if(file.exists(output_file)){
    print(glue::glue("Output file {output_file} exists, skipping"))
    next
  }
  # skip miami
  if(metro == "miami"){
    print("Skipping miami")
    next
  }


#### read in altos data ####
altos_file = glue("{altos_input_dir}{metro}_metro_all_years_clean_addys.csv")
corelogic_file = glue("{corelogic_input_dir}/cleaned_{fips_code}.csv")
xwalk_listings_file = glue("{xwalk_dir}/{metro}_{fips_code}_altos_corelogic_xwalk_listing.csv")

altos = fread(altos_file)
corelogic = fread(corelogic_file)
xwalk_listings = fread(xwalk_listings_file)

# merge altos and corelogic on address
altos_m = altos %>%
  merge(xwalk_listings[unique ==T] %>% select(-contains("city")), by = "listing_id") %>%
  merge(corelogic[], by = "clip", suffixes = c("_altos", "_corelogic"))

# clean altos
altos_m_dates <- str_match(altos_m$date, "([0-9]{4})-([0-9]{2})-([0-9]{2})")
altos_m[,year := as.numeric(altos_m_dates[,2])]
altos_m[,month := as.numeric(altos_m_dates[,3])]
altos_m[,day := as.numeric(altos_m_dates[,4])]
altos_m[,ym := year + (month-1)/12]
altos_m[,ymd := year + (month-1)/12 + (day-1) / 365]
altos_m[,price :=as.numeric(price)]
altos_m[,beds_txt := impute_units(property_name)]
altos_m[,beds_imp := coalesce(beds, beds_txt, bedrooms_all_buildings)]
altos_m[,baths_imp := coalesce(baths, total_bathrooms_all_buildings)]
altos_m[,price_per_bed :=as.numeric(price)/beds_imp]
#altos_m[,address_id := .GRP, by = .(pm.house, pm.street, pm.streetSuf, pm.preDir, pm.sufDir,pm.zip)]
altos_m[,bed_address_id := .GRP, by = .(clip, beds_imp)]

altos_m[,census_block := paste0(
  str_pad(fips_code, width=5, side="left", pad="0"),
  str_pad(census_id, width=10, side="left", pad="0")
)]
altos_m[,census_bg := str_sub(census_block, 1, 12)]
altos_m[,census_tract := str_sub(census_block, 1, 11)]
altos_m[,voucher := str_detect(property_name, regex("section 8|section eight|sec 8|housing choice voucher|hcv|voucher", ignore_case = T))]
altos_m[,ever_voucher := max(voucher), by = clip]
altos_m[,beds_imp_round := round(beds_imp)]
altos_m[,baths_imp_round := case_when(
  baths_imp <= 4~ round(baths_imp,1),
  baths_imp > 4 & baths_imp <=6~ round(baths_imp),
  baths_imp > 6 ~ 6,
  TRUE ~NA_real_
)]

altos_m[,beds_imp_round := case_when(
  beds_imp <= 6~ round(beds_imp),
  beds_imp > 6 ~ 6,
  TRUE ~NA_real_
)]

# make yearly aggs
altos_m_year_aggs = altos_m[price >= 500 & price <= 6000,
                            list(
                              price = median(price),
                              month = first(month),
                              beds_imp_round = first(beds_imp_round),
                              baths_round = first(baths_imp_round),
                              ever_voucher = first(ever_voucher),
                              voucher = max(voucher),
                              clip = first(clip),
                              census_tract = first(census_tract),
                              census_bg = first(census_bg)
                            ), by = .(bed_address_id, year)]

# really dumb but now remerge the corelogic data
altos_m_year_aggs = altos_m_year_aggs %>%
  merge(corelogic %>% select(clip,total_value_calculated:total_number_of_efficiency_units,contains("code") ),
        by = "clip", suffixes = c("_altos", "_corelogic"))

altos_m_year_aggs[,list(sum(ever_voucher),.N), by = year][,per := round(V1/N,3)][order(year)]
altos_m_year_aggs[,list(sum(voucher),.N), by = year][order(year)]
altos_m[,list(sum(ever_voucher),.N), by = year][order(year)]
altos_m_year_aggs[,stories_num := as.numeric(stories_number)]
fwrite(
  altos_m_year_aggs,
 output_file
)
}
