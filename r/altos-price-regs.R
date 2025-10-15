library(data.table)
library(tidyverse)
library(sf)
library(tidycensus)
library(glue)
library(fixest)
source("r/helper-functions.R")
#### files ####
indir = '/Users/joefish/Desktop/data/altos-corelogic-agg'
df_list = lapply(list.files(indir, full.names = T), fread)
# fix census tract to be numeric
df_list = lapply(df_list, function(x) {
  x[,census_tract := as.numeric(census_tract)]
  x[,census_bg := as.numeric(census_bg)]
  return(x)
})
altos_m_year_aggs = rbindlist(df_list, use.names = T, fill = T)

altos_m_year_aggs[year <= 2018][,list(
  mean_voucher = mean(ever_voucher, na.rm = T),
  num_voucher = sum(ever_voucher, na.rm = T),
  mean_price = mean(price, na.rm = T),
  mean_price_voucher = mean(price[ever_voucher == 1], na.rm = T),
  mean_price_no_voucher = mean(price[ever_voucher == 0], na.rm = T),
  num_listings = .N
), by = .(fips_code)][,
                      voucher_ratio := mean_price_voucher / mean_price_no_voucher][
                        order(voucher_ratio)]

# get tract income from acs for all counties in altos_m_year_aggs
#states = unique(str_sub(str_pad(altos_m_year_aggs$census_tract,11,"left","0"),1,2))
states = fips_codes %>% filter(state_code <= 56) %>% pull(state_code) %>% unique()
tract_inc <- tidycensus::get_acs(
  geography = "tract",
  variables = "B19013_001",
  year = 2018,
  state = states,
  #county  = unique(altos_m_year_aggs$fips_code),
  geometry = FALSE,
  output = "wide"
)

altos_m_year_aggs = merge(
  altos_m_year_aggs,
  tract_inc[,c("GEOID", "B19013_001E")] %>% mutate(census_tract = as.numeric(GEOID)),
  by.x = "census_tract",
  by.y = "census_tract",
  all.x = T
)

altos_m_year_aggs[,income_quartile := ntile(B19013_001E,4), by = fips_code]

# regressions
m1 <- feols(log(price) ~ ever_voucher*i(fips_code, ref = 17031) |baths_round+ beds_imp_round + year + month ,
            data = altos_m_year_aggs[year <= 2018 & income_quartile <= 2],
            weights = ~ log(universal_building_square_feet),
            cluster = ~clip)
m2 <- feols(log(price) ~ ever_voucher + ever_voucher:i(income_quartile,ref = 1) | census_tract + beds_imp_round + baths_round + year + month , data = altos_m_year_aggs[year <= 2018],
            weights = ~ log(universal_building_square_feet),
            cluster = ~clip)
m3 <- feols(log(price) ~ ever_voucher + ever_voucher:i(income_quartile,ref = 1) | census_bg + beds_imp_round + baths_round + year + month , data = altos_m_year_aggs[year <= 2018],
            cluster = ~clip,
            weights = ~ log(universal_building_square_feet),
            )
m4 <- feols(log(price) ~ ever_voucher +ever_voucher:i(fips_code, ref = 17031) +log(land_square_footage)+
              log(universal_building_square_feet) + log(total_value_calculated) + year_built + year_built^2  |
              census_tract + beds_imp_round +baths_round + beds_imp_round +property_indicator_code+ month ^year ,
            data = altos_m_year_aggs[year <= 2018 & income_quartile <= 2],
            weights = ~ log(universal_building_square_feet),
            cluster = ~clip)

summary(m1)
summary(m2)
summary(m3)
summary(m4)

library(fwlplot)
altos_m_year_aggs[,log_price := log(price)]
fwlplot(log_price ~ ever_voucher |beds_imp_round + baths_round +census_tract, altos_m_year_aggs[])

ggplot(
  altos_m_year_aggs[year <= 2018],
  aes(x = baths_round, y = log(price), color = as.factor(voucher), group = as.factor(voucher))
) +
  geom_point() +
  geom_smooth(method = "lm", se = F) +
  labs(
    title = "Price by Baths and Beds",
    x = "Baths",
    y = "Log Price"
  ) +
  theme_minimal(
)


