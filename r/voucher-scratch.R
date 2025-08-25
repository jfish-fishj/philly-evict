# download Chicago census blkgroup shapefile
library(tidycensus)
library(tidyverse)
library(sf)
library(data.table)
# Load the shapefile for Chicago census block groups
Chicago_bg <- get_acs(geography = "block group",
                     variables = "B01003_001",
                     state = "IL",
                     county = "Cook",
                     geometry = TRUE,
                     year = 2021)

# export as phill
write_sf(Chicago_bg, "~/Desktop/data/Chicago-evict/Chicago_bg.shp")
Chicago = fread(("/Users/joefish/Desktop/data/altos/cities/chicago_metro_all_years.csv"))

# make altos data into sf using geo_lat, geo_long
Chicago_sf = st_as_sf(Chicago[!is.na(geo_lat) & !is.na(geo_long),.(geo_lat, geo_long)] %>% unique(),
                     coords = c("geo_long", "geo_lat"),
                     remove = F,
                     crs = st_crs(Chicago_bg))

# merge on Chicago_bg
Chicago_sf <- st_join(Chicago_sf, Chicago_bg, join = st_within)

# merge Chicago sf onto Chicago
Chicago_m <- merge(Chicago, Chicago_sf, by = c("geo_long", "geo_lat"), all.x = TRUE)
Chicago_m[,voucher := str_detect(property_name, regex("voucher|section eight|section 8", ignore_case = T))]
Chicago_m[,ever_voucher := max(voucher), by = .(str_to_upper(street_address))]
# number + percent of vouchers by year
Chicago_m[,list(
  num_voucher = sum(ever_voucher, na.rm = TRUE),
  pct_voucher = mean(ever_voucher, na.rm = TRUE)
), by = year][order(year)]

unique(Chicago_m, by = c('unit_id', 'year'))[,list(
  num_voucher = sum(ever_voucher, na.rm = TRUE),
  pct_voucher = mean(ever_voucher, na.rm = TRUE)
), by = year][order(year)]

m1 <- fixest::feols(log(price) ~ voucher + beds + beds^2 + baths + baths^2|  year + month,
              data = Chicago_m[beds <= 5 & baths <= 5 & price > 100 & price <= 5000& year <= 2018] )

m2 <- fixest::feols(log(price) ~ voucher + beds + beds^2 + baths + baths^2|  year + month,
              data = Chicago_m[beds <= 5 & baths <= 5 & price > 100 & price <= 5000& year <= 2018] )

Chicago_m_unique <- Chicago_m %>% distinct(unit_id,year,beds,baths, .keep_all = TRUE)

m2 <- fixest::feols(log(price) ~ ever_voucher + beds + beds^2 + baths + baths^2|  year + month,
                    data = Chicago_m_unique[beds <= 5 & baths <= 5 & price > 100 & price <= 5000& year <= 2018] )

m3 <- fixest::feols(log(price) ~ ever_voucher + beds + beds^2 + baths + baths^2| GEOID+ year + month,
                    data = Chicago_m_unique[beds <= 5 & baths <= 5 & price > 100 & price <= 5000& year <= 2018] )

m4 <- fixest::feols(log(price) ~ ever_voucher + beds + beds^2 + baths + baths^2| GEOID ^ year + month,
              data = Chicago_m_unique[beds <= 5 & baths <= 5 & price > 100 & price <= 5000 & year <= 2018] )

m5 <- fixest::feols(log(price) ~ ever_voucher*i(year, ref = 2015) + beds + beds^2 + baths + baths^2|street_address+ GEOID ^ year + month,
                    data = Chicago_m_unique[beds <= 5 & baths <= 5 & price > 100 & price <= 5000 & year <= 2018] )

summary(m2)
summary(m3)
summary(m4)
coeftable(m4, keep = "voucher")
coeftable(m5, keep = "voucher")


# get the number of housing units in san francisco by zipcode in the
# 2000, 2010, and 2020 census

cen2000 <- get_decennial(geography = "zcta",
                             variables = c("population"= "H011001",
                                          "housing_units"= "H001001"),
                             # state = "CA",
                             # county = "San Francisco",
                         output = "wide",
                             year = 2000,
                             geometry = F)

cen2010 <- get_decennial(geography = "zcta",
                         variables = c("population"= "H011001",
                                       "housing_units"= "H001001"),
                             # state = "CA",
                             # county = "San Francisco",
                             output = "wide",
                             year = 2010,
                             geometry = F)

cen2020 <- get_acs(geography = "zcta",
                             variables = c(
                             "population"=  "B25026_001",
                             "housing_units"=  "B25002_001"
                             ),
                   output = "wide",
                             # state = "CA",
                             # county = "San Francisco",
                             year = 2022)

# bind together
cen_all <- rbindlist(list(cen2000 %>% mutate(year = 2000,

                                                GEOID = as.character(GEOID)),
                          cen2010 %>% mutate(year = 2010,

                                   GEOID = as.character(GEOID)),
                          cen2020%>% mutate(year = 2020,
                                   variable = "B25002_001",
                                  population = populationE,
                                  housing_units = housing_unitsE,
                                   GEOID = as.character(GEOID))
                            ), fill = TRUE)
cen_all = cen_all %>%merge(zipcodeR::zip_code_db %>% select(zipcode, major_city, county, state),
                           by.x = "GEOID", by.y = "zipcode", all.x = TRUE)

cen_all_san_fran = cen_all %>% filter(str_detect(major_city,"San Fran") & state == "CA")
setDT(cen_all_san_fran)

cen_all_san_fran[,change_housing_rel2000 := (housing_units[year == 2020] - housing_units[year == 2000]), by = GEOID]
cen_all_san_fran[,change_population_rel2000 := (population[year == 2020] - population[year == 2000]), by = GEOID]

cen_all_san_fran[year == 2020,list(mean(change_housing_rel2000,na.rm = T))]
cen_all_san_fran[year == 2020,list(mean(change_population_rel2000,na.rm = T))]
cen_all_san_fran[,change_housing_rel2000_per_year := change_housing_rel2000 / 22]

# repeat for rel 2010

cen_all_san_fran[,change_housing_rel2010 := (housing_units - first(housing_units[year == 2010])), by = GEOID]
cen_all_san_fran[,change_population_rel2010 := (population - first(population[year == 2010])), by = GEOID]
cen_all_san_fran[year == 2020,list(mean(change_housing_rel2010,na.rm = T))]
cen_all_san_fran[year == 2020,list(mean(change_population_rel2010,na.rm = T))]
cen_all_san_fran[,change_housing_rel2010_per_year := change_housing_rel2010 / 12]


# bin housing by less than 0, 500, 2000, 5000, 10000, 10000 +
cen_all_san_fran[,change_housing_bin := cut(change_housing_rel2010,
                                             breaks = c(-Inf,0, 500,1000, 2000, 5000, 10000, Inf),
                                             labels = c("Decrease",
                                                        "Increase 0-500",
                                                        "Increase 501-1000",
                                                        "Increase 1001-2000",
                                                        "Increase 2001-5000",
                                                        "Increase 5001-10000",
                                                        "Increase > 10000"))]

# repeat for per year (so divide bins by 20)
cen_all_san_fran[,change_housing_bin_per_year := cut(change_housing_rel2010_per_year,
                                             breaks = c(-Inf,0, 25,50, 100, 250, 500, Inf),
                                             labels = c("Decrease",
                                                        "Increase 0-25",
                                                        "Increase 26-50",
                                                        "Increase 51-100",
                                                        "Increase 101-250",
                                                        "Increase 251-500",
                                                        "Increase > 500"))]


# merge on zip code shapefile
library(zipcodeR)
library(tigris)
# Load the shapefile for San Francisco zip codes
sf_zip <- tigris::zctas(state = "CA", year = 2010, class = "sf", cache = T)

cen_all_san_fran_sf <- merge(
  sf_zip,
  cen_all_san_fran,
  by.x = "ZCTA5CE10",
  by.y = "GEOID"
)

# map

library(ggplot2)
library(ggthemes)

# get centroid for each zip code
cen_all_san_fran_sf <- cen_all_san_fran_sf %>%
  mutate(centroid = st_centroid(geometry)) %>%
  mutate(centroid_long = st_coordinates(centroid)[,1],
         centroid_lat = st_coordinates(centroid)[,2])

ggplot() +
  geom_sf(data = cen_all_san_fran_sf %>%
            filter(year == 2020 & population > 0 &
                     !ZCTA5CE10 %in% c(94130,94080) & !is.na(change_housing_bin)),
          aes(fill = change_housing_bin),
          color = "white",
          size = 0.1) +
  geom_text(data = cen_all_san_fran_sf %>%
              filter(year == 2020 & population > 0 &
                       !ZCTA5CE10 %in% c(94130,94080) & !is.na(change_housing_bin)),
            aes(x = centroid_long, y = centroid_lat,
                label = round(change_housing_rel2010)),
            size = 3, color = "black") +
  scale_fill_brewer(palette = "RdYlGn",
                    name = "Change in Housing Units (2010-2022)") +
  labs(title = "Change in Housing Units in San Francisco\nby Zip Code (2010-2022)",
       subtitle = "Data from the U.S. Census Bureau") +
  theme_minimal() +
  guides(fill = guide_legend(nrow = 4))+
  theme(legend.position = "bottom",
        # change number of legend rows
        plot.title = element_text(hjust = 0.5, size = 16),
        plot.subtitle = element_text(hjust = 0.5, size = 12))

# repeat but average units completed per year
ggplot() +
  geom_sf(data = cen_all_san_fran_sf %>%
            filter(year == 2020 & population > 0 &
                     !ZCTA5CE10 %in% c(94130,94080) & !is.na(change_housing_bin_per_year)),
          aes(fill = change_housing_bin_per_year),
          color = "white",
          size = 0.1) +
  geom_text(data = cen_all_san_fran_sf %>%
              filter(year == 2020 & population > 0 &
                       !ZCTA5CE10 %in% c(94130,94080) & !is.na(change_housing_bin_per_year)),
            aes(x = centroid_long, y = centroid_lat,
                label = round(change_housing_rel2010_per_year)),
            size = 3, color = "black") +
  scale_fill_brewer(palette = "RdYlGn",
                    name = "Change in Housing Units (2010-2022)\nPer Year") +
  labs(title = "Average Change in Housing Units in San Francisco\nby Zip Code (2010-2022)",
       subtitle = "Data from the U.S. Census Bureau") +
  theme_minimal() +
  guides(fill = guide_legend(nrow = 4))+
  theme(legend.position = "bottom",
        # change number of legend rows
        plot.title = element_text(hjust = 0.5, size = 16),
        plot.subtitle = element_text(hjust = 0.5, size = 12))


v17 <- load_variables(2000, "sf1", cache = TRUE)
v20 <- load_variables(2020, "acs5", cache = TRUE)
assessments = fread("~/Downloads/assessments.csv")
permits = fread("~/Downloads/permits.csv")
complaints = fread("~/Downloads/complaints.csv")
complaints[,complaint_year := as.numeric(str_sub(complaintdate, 1,4))]
complaints[,.N, by = complaint_year][order(complaint_year)][,.(complaint_year, N)]


bps_houston = bps[PLACE_NAME == "Houston" &COUNTY_NAME=="Harris County" & PERIOD=="Annual"]
ggplot(bps_houston, aes(x = SURVEY_DATE)) +
  geom_line(aes(y = UNITS_2_UNITS, color = "2 units")) +
  geom_line(aes(y = UNITS_3_4_UNITS, color = "3-4 units"))

