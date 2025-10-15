pa_parquet = arrow::read_parquet(
  "~/Downloads/PA.parquet"
)

setDT(pa_parquet)
str(head(pa_parquet))

la[,decade_blt :=
  case_when(
    year_built==0 ~ "unknown",
    year_built < 1930 ~ "pre-1930",
    year_built >= 1930 & year_built < 1940 ~ "1930s",
    year_built >= 1940 & year_built < 1950 ~ "1940s",
    year_built >= 1950 & year_built < 1960 ~ "1950s",
    year_built >= 1960 & year_built < 1970 ~ "1960s",
    year_built >= 1970 & year_built < 1980 ~ "1970s",
    year_built >= 1980 & year_built < 1990 ~ "1980s",
    year_built >= 1990 & year_built < 2000 ~ "1990s",
    year_built >= 2000 & year_built < 2010 ~ "2000s",
    year_built >= 2010 ~ "2010+",
    TRUE ~ NA_character_
  )]

fixest::feols(
  log_med_rent ~ i(decade_blt, ref = "pre-1930") #+ filing_rate_sq*i(source,ref = "evict")
  |year+num_units_bins+building_code_description_new_fixed+beds_imp_first+baths_first  #num_units_bins+ #month +
  , data = analytic_df[source == "altos" ],
 # weights = ~(num_units_imp),
  cluster = ~PID,
  combine.quick = T
)

fixest::feols(
  log_med_rent ~ i(decade_blt, ref = "pre-1930") #+ filing_rate_sq*i(source,ref = "evict")
  |GEOID^year+num_units_bins+building_code_description_new_fixed+beds_imp_first+baths_first   #num_units_bins+ #month +
  , data = analytic_df[source == "altos" ],
  weights = ~(num_units_imp),
  cluster = ~PID,
  combine.quick = T
)

houston[,log_price := log(price)]
la[,log_price := log(price)]

fixest::feols(
  log_price ~ i(decade_blt, ref = "pre-1930") #+ filing_rate_sq*i(source,ref = "evict")
  |census_tract^year+beds_imp_round+baths_round  #num_units_bins+ #month +
  , data = temp[ ],
  # weights = ~(num_units_imp),
  cluster = ~clip,
  combine.quick = T
)

houston[,quantile(price,na.rm=T)]
temp= rbindlist(list(houston[,city := "houston"], la[,city := "los angeles"]), use.names = TRUE, fill = TRUE)
bed_bath_reg <- fixest::feols()
temp[,decade_blt_numeric := round(year_built / 1000,2) * 1000]

ages_dist = temp %>%
  filter(year_built>= 1900 & year %in% c(2018, 2019))%>%
  ggplot(aes(x = year_built, group =city, color = city ))+
  geom_density() +
  labs(
    title = "Density of Year Built",
    x = "Year Built",
    y = "Density",
    caption = "Rental Listing Data from Altos; Year Built data from Core Logic"
  ) +
  theme_philly_evict()

prices_dist = temp %>%
  filter(year_built>= 1900 & year %in% c(2018, 2019))%>%
  ggplot(aes(x = price, group =source, color = source ))+
  # make x axis logs
  scale_x_log10() +
  geom_density() +
  labs(
    title = "Density of Rental Listings",
    x = "Log Price",
    y = "Density"
  ) +
  theme_philly_evict()

cowplot::plot_grid(
  ages_dist + ggtitle("Density of Year Built"),
  prices_dist + ggtitle("Density of Log Price"),
  ncol = 2
)

age_aggs = temp[year %in% c(2018, 2019) & beds_imp_round == 2,list(
  mean_price = mean(price)
), by = .(source, decade_blt_numeric)]


age_aggs %>%
  filter(decade_blt_numeric >= 1930)%>%
  ggplot(aes(x = decade_blt_numeric, y = mean_price, fill = source)) +
  geom_col(position = "dodge")

wave2 = fread('/Users/joefish/Downloads/ICPSR_38483/DS0002/38483-0002-Data.tsv')
wave1 = fread('/Users/joefish/Downloads/ICPSR_38483/DS0001/38483-0001-Data.tsv')
wave2[,CT_ID_10 := paste0(
  str_pad(state,2, "left","0"),
  str_pad(t_county,3, "left","0"),
  str_pad(tract2,6, "left","0")
)]

acs_vars = load_variables(2013,"acs5")
var_list = c(
  "medhhinc"  ="B25119_001",
  "total_vacant" = 'B25002_003',
  "poverty_pop" ='B06012_002',
  "pop" ='B01001A_001',
  "employed_pop" = 'B23025_004',
  'gini'='B19083_001',
  "pop_bachelors" = 'B16010_041',
  "pop_married"='B11013_002',
  "owner_occ"='B25003_002',
  "total_hh"="B25003_001",
  "housing_units"='B25002_001',
  "med_age"='B01002_001'
)
states =  tidycensus::fips_codes %>% filter(state_code <= 56) %>% pull(state_code) %>% unique()
acs2013 <- get_acs(geography = "tract",
                    variables =var_list,
                    year = 2013,
                    output = "wide",
                    state = states,
                    cache = T,
                    geometry = F)

setDT(acs2013)
acs2013[,sum(popE), by = GEOID %in% wave2$CT_ID_10]


#
nhgis = fread("~/Downloads/nhgis0034_csv/nhgis0034_ts_nominal_nation.csv")
# residualize rents
resid_reg <- feols(log_med_rent ~ log(total_area)|
        year_blt_decade+
        num_stories_bin+
        source +
        quality_grade_fixed +
        exterior_condition
      +building_code_description_new_fixed
      +num_units_bins +GEOID
  , data = bldg_panel[ ],
  weights = ~(num_units_imp),
  cluster = ~PID,
  combine.quick = F
)

bldg_panel[,price_resid := resid(resid_reg)]
bldg_panel[,high_filing := filing_rate >= 0.10]
bldg_panel[,quantile(filing_rate,na.rm=T)]
zip_rent_aggs = bldg_panel[,list(
  mean_rent = weighted.mean(price_resid, num_units_imp, na.rm=T),
  num_units_with_rent = sum(!is.na(med_price) & !is.infinite(med_price) & med_price>0 & num_units_imp>0),
  mean_rent_high_filing =  weighted.mean(price_resid[high_filing], num_units_imp[high_filing], na.rm=T),
  num_units_with_rent_high_filing = sum(!is.na(med_price[high_filing]) & !is.infinite(med_price[high_filing]) & med_price[high_filing]>0 & num_units_imp[high_filing]>0)
),by = .(year, pm.zip)]

zip_unit_aggs = rent_list$lic_long_min[,list(
  total_units = sum(numberofunits, na.rm=T),
  total_properties = .N,
  total_units_high_filing = sum(numberofunits[PID %in% bldg_panel[high_filing==T, PID] ], na.rm=T),
  total_properties_high_filing = sum(PID %in% bldg_panel[high_filing==T, PID] )
),by = .(year, pm.zip)]

zip_aggs = merge(zip_rent_aggs,
                 zip_unit_aggs,
                 by = c("year","pm.zip"),
                 all.x = T)

zip_aggs_pre_post_covid = zip_aggs[year %in% c(2018, 2019, 2022, 2023), list(
  mean_rent_pre_covid = mean(mean_rent[year %in% c(2018, 2019)], na.rm=T),
  mean_rent_post_covid = mean(mean_rent[year %in% c(2022, 2023)], na.rm=T),
  mean_rent_high_filing_pre_covid = mean(mean_rent_high_filing[year %in% c(2018, 2019)], na.rm=T),
  mean_rent_high_filing_post_covid = mean(mean_rent_high_filing[year %in% c(2022, 2023)], na.rm=T),
  total_units_with_rent_pre_covid = mean(num_units_with_rent[year %in% c(2018, 2019)], na.rm=T),
  total_units_with_rent_high_filing_pre_covid = mean(num_units_with_rent_high_filing[year %in% c(2018, 2019)], na.rm=T),
  total_units_with_rent_post_covid = mean(num_units_with_rent[year %in% c(2022, 2023)], na.rm=T),
  total_units_with_rent_high_filing_post_covid = mean(num_units_with_rent_high_filing[year %in% c(2022, 2023)], na.rm=T),
  total_units_pre_covid = mean(total_units[year %in% c(2018, 2019)], na.rm=T),
  total_units_post_covid = mean(total_units[year %in% c(2022, 2023)], na.rm=T),
  total_units_high_filing_pre_covid = mean(total_units_high_filing[year %in% c(2018, 2019)], na.rm=T),
  total_units_high_filing_post_covid = mean(total_units_high_filing[year %in% c(2022, 2023)], na.rm=T),
  total_properties_pre_covid = mean(total_properties[year %in% c(2018, 2019)], na.rm=T),
  total_properties_post_covid = mean(total_properties[year %in% c(2022, 2023)], na.rm=T),
  total_properties_high_filing_pre_covid = mean(total_properties_high_filing[year %in% c(2018, 2019)], na.rm=T),
  total_properties_high_filing_post_covid = mean(total_properties_high_filing[year %in% c(2022, 2023)], na.rm=T)
), by = pm.zip]

zip_aggs_pre_post_covid[,change_in_rent := mean_rent_post_covid - mean_rent_pre_covid]
zip_aggs_pre_post_covid[,change_in_rent_high_filing := mean_rent_high_filing_post_covid - mean_rent_high_filing_pre_covid]
zip_aggs_pre_post_covid[,change_in_units := total_units_post_covid - total_units_pre_covid]
zip_aggs_pre_post_covid[,change_in_units_high_filing := total_units_high_filing_post_covid - total_units_high_filing_pre_covid]
zip_aggs_pre_post_covid[,change_in_properties := total_properties_post_covid - total_properties_pre_covid]
zip_aggs_pre_post_covid[,change_in_properties_high_filing := total_properties_high_filing_post_covid - total_properties_high_filing_pre_covid]

zip_aggs_pre_post_covid[,quantile(total_units_with_rent_high_filing_pre_covid)]
zip_aggs_pre_post_covid[,quantile(total_units_high_filing_pre_covid,na.rm=T)]

zip_aggs_pre_post_covid[total_units_with_rent_high_filing_pre_covid >= 0 & abs(change_in_rent_high_filing)<=0.2 & abs(change_in_rent)<=0.2,
                        .(change_in_units_high_filing,change_in_rent_high_filing,
                          change_in_properties_high_filing,change_in_properties,
                          change_in_units, change_in_rent)] %>% cor()

ggplot(zip_aggs_pre_post_covid, aes(x = change_in_units,
                                    y = change_in_rent)) +
  geom_point() +
  geom_smooth(method = "lm") +
  labs(
    title = "Change in Rental Units vs Change in Rent (2018-2019 to 2022-2023)",
    x = "Change in Rental Units",
    y = "Change in Rent"
  ) +
  theme_philly_evict()

ggplot(zip_aggs_pre_post_covid[total_units_with_rent_high_filing_pre_covid >= 10
                               & abs(change_in_rent)<=0.3
                               & abs(change_in_rent_high_filing)<=0.3],
       aes(x = change_in_units_high_filing, y = change_in_rent_high_filing)) +
  geom_point() +
  geom_smooth(method = "lm") +
  labs(
    title = "Change in Rental Units (High Filing) vs Change in Rent (2018-2019 to 2022-2023)",
    x = "Change in Rental Units (High Filing)",
    y = "Change in Rent (High Filing)"
  ) +
  theme_philly_evict()

ggplot(zip_aggs_pre_post_covid[total_units_with_rent_high_filing_pre_covid >= 5 &
                                 abs(change_in_rent)<=0.3], aes(x = change_in_units_high_filing, y = change_in_rent)) +
  geom_point() +
  geom_smooth(method = "lm") +
  labs(
    title = "Change in Rental Units (High Filing) vs Change in Rent (2018-2019 to 2022-2023)",
    x = "Change in Rental Units (High Filing)",
    y = "Change in Rent (High Filing)"
  ) +
  theme_philly_evict()

