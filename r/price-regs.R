library(data.table)
library(tidyverse)
library(sf)
library(tidycensus)
library(fixest)

source('r/helper-functions.R')
indir = "/Users/joefish/Desktop/data/philly-evict/processed"
analytic_df = fread(file.path(indir, "analytic_df.csv"))
bldg_df = fread(file.path(indir, "bldg_panel.csv"))
rent_list = fread(file.path(indir, "license_long_min.csv"))
parcels = fread(file.path(indir, "parcel_building_2024.csv"))



analytic_df[,high_filing := ifelse(filing_rate > 0.1, 1, 0)]
bldg_panel[,high_filing := ifelse(filing_rate > 0.1, 1, 0)]
bldg_panel[,filing_rate_year := num_filings / num_units_imp]
bldg_panel[,high_filing_year := ifelse(filing_rate_year > 0.1, 1, 0)]
bldg_panel[,last_obs := year == max(year) | year == 2019, by = PID]
bldg_panel[order(PID, year), cumsum_filings := cumsum(num_filings), by = PID]
bldg_panel[,rel_year := year - 2005]
bldg_panel[order(PID, year), cumsum_filing_rate :=cumsum_filings/(rel_year)/num_units_imp]
bldg_panel[,sfh := num_units_imp == 1]
bldg_panel[,year_blt_decade := floor(year_built / 10) * 10]
bldg_panel[,year_blt_decade := fifelse(year_built < 1900, "pre-1900", as.character(year_blt_decade))]
bldg_panel[,num_stories_bin := case_when(
  number_stories == 1 ~ "1",
  number_stories <= 3 ~ "2-3",
  number_stories <= 5 ~ "4-5",
  number_stories <= 10 ~ "6-10",
  number_stories > 10 ~ "11+",
  TRUE ~ NA_character_
)]

bldg_panel[,total_permits_all := sum(total_permits), by = PID]
bldg_panel[,total_violations_all := sum(total_violations), by = PID]
bldg_panel[,total_complaints_all := sum(total_complaints), by = PID]
bldg_panel[,total_investigations_all := sum(total_investigations), by = PID]

# per unit
bldg_panel[,permits_per_unit := total_permits_all/num_units_imp]
bldg_panel[,violations_per_unit := total_violations_all/num_units_imp]
bldg_panel[,severe_violations_per_unit := sum(
  (unsafe_violation_count+
     hazardous_violation_count +imminently_dangerous_violation_count
  )
)/num_units_imp, by = PID]
bldg_panel[,complaints_per_unit := total_complaints_all/num_units_imp]
bldg_panel[,investigations_per_unit := total_investigations_all/num_units_imp]

# make dummies for any complaints, permits, etc
bldg_panel[,ever_permit := fifelse(total_permits_all > 0, 1, 0)]
bldg_panel[,ever_violations := fifelse(total_violations_all > 0, 1, 0)]
bldg_panel[,ever_complaints := fifelse(total_complaints_all > 0, 1, 0)]
bldg_panel[,ever_investigations := fifelse(total_investigations_all > 0, 1, 0)]

bldg_panel[,any_unsafe_hazordous_dangerous := any(unsafe_violation_count > 1 |
                                                     hazardous_violation_count > 1 | imminently_dangerous_violation_count > 1
),
by = PID]




# bin filing rates into 0,5,10,20,50+
bldg_panel[,filing_rate_cuts := cut(
  filing_rate,
  breaks = c(-Inf,  0.1, 0.2, 0.3, Inf),
  labels = c("0-10%",  "10-20%", "20-30%", "30%+"),
  include.lowest = TRUE
) ]
bldg_panel[,post_covid := (year >= 2021)]

# break unit bins into 5 categories
m1 <- fixest::feols(
  log_med_rent ~filing_rate_cuts+#*post_covid +
   #+filing_rate_sq #+  ever_voucher*source
+ num_units_imp
+ num_units_imp^2
#+ sfh#*source
 #   + year_built #* ever_permit
 # + year_built^2
 #+ permits_per_unit#*i(source,ref = "evict")
 +severe_violations_per_unit#*i(source,ref = "evict")
 # +i(beds_imp_first_fixed, ref = 0)
 # +i(baths_first_fixed, ref = 1)
 #+ complaints_per_unit#*i(source,ref = "evict")
# + permits_per_unit
 #+ total_investigations_per_unit
# + severe_violations_per_unit
 #+poly(number_stories,3)#*i(source,ref = "evict")
+log(total_area)
 |GEOID^year+  #num_units_bins+ #month +
 #num_units_bins+#^source+
  year_blt_decade+
  num_stories_bin+
  #number_of_bedrooms +
  #source +
   quality_grade_fixed +
  exterior_condition
   +building_code_description_new_fixed
 , data = bldg_panel[source == "evict" &
                       #year <= 2019 &
                       #year>=2014 &
                    # &num_units_imp > 50
                    #(last_obs == T) &
                         filing_rate <=1  ],
weights = ~(num_units_imp),
  cluster = ~PID,
  combine.quick = T
)



etable(m1, keep = "%filing_rate_cuts",
       dict = c("filing_rate_cuts" = "Filing Rate Category",
                "filing_rate_cuts0-10%" = "Filing Rate: 0-10%",
                "filing_rate_cuts10-20%" = "Filing Rate: 10-20%",
                "filing_rate_cuts20-30%" = "Filing Rate: 20-30%",
                "filing_rate_cuts30%+" = "Filing Rate: 30%+",
                "num_units_imp" = "Number of Units",
                "num_units_imp^2" = "Number of Units Squared",
                "severe_violations_per_unit" = "Severe Violations per Unit",
                "log(total_area)" = "Log Total Area",
                "source" = "Data Source",
                "quality_grade_fixed" = "Quality Grade",
                "exterior_condition" = "Exterior Condition",
                "building_code_description_new_fixed" = "Building Type",
                "year_blt_decade" = "Decade Built",
                "num_stories_bin" = "Number of Stories",
                "GEOID" = "Census Block Group"
                ),
       tex = T
       ) %>%
  writeLines("tables/hedonic_filing_rate_cuts.tex")


coeftable(m1, keep = "filing|voucher")
#View(analytic_df[order(desc(abs(resids)))][1:100])

quantile(m1$residuals)
summary(m1)


m2 = fixest::feols(
  log_med_rent ~ filing_rate + filing_rate_sq + num_units + num_units^2  |
    GEOID^year+type_heater +quality_grade_fixed +year_blt_decade + num_stories_bin+
    view_type + building_code_description_new_fixed+
    general_construction ,
  data = bldg_panel[source == "evict" & filing_rate < 1],
  #weights = ~num_units,
  cluster = ~PID,
  combine.quick = F
)

# same thing but full sample
m3 = fixest::feols(
  log_med_rent ~filing_rate | year +source,
  data = analytic_df[ filing_rate < 1  ],
  #weights = ~num_units,
  cluster = ~PID
)

m4 = fixest::feols(
  log_med_rent ~ filing_rate + filing_rate_sq + num_units + num_units^2  |
    GEOID^year+type_heater +quality_grade_fixed +year_blt_decade + num_stories_bin+
    view_type + building_code_description_new_fixed+
    general_construction ,
  data = bldg_panel[ filing_rate < 1],
  #weights = ~num_units,
  cluster = ~PID,
  combine.quick = F
)

# m5 = fixest::feols(
#   log_med_rent ~ filing_rate*source  +num_units_bins+ poly(num_units,2) + poly(year_built,2)+poly(number_stories,2) +
#     poly(beds_imp_first_pred,2) + poly(baths_first_pred,2)|
#     GEOID^year+type_heater +quality_grade_fixed +
#     view_type + building_code_description_new_fixed+
#     general_construction ,
#   data = analytic_df[filing_rate>0 & year <= 2018 & filing_rate < 1& !is.na(baths_first_pred)],
#   #weights = ~num_units,
#   cluster = ~PID,
#   combine.quick = F
# )
#
# m6 = fixest::feols(
#   log_med_rent ~ filing_rate + ever_voucher  +num_units_bins+ + poly(year_built,2)+#poly(number_stories,2) +
#     poly(beds_imp_first_pred,2) + poly(baths_first_pred,2)|
#     GEOID^year+type_heater +quality_grade_fixed +source+
#     view_type + building_code_description_new_fixed+
#     general_construction ,
#   data = analytic_df[ year <= 2018  & filing_rate < 1& !is.na(baths_first_pred)],
#   weights = ~num_units,
#   cluster = ~PID,
#   combine.quick = F
# )

# export tables:
# make table
setFixest_dict(
  c(
    "source"= "Data Source",
    "sourceevict"= "Eviction Rental",
    "year" = "Year",
    "log_med_rent" = "Log(Price)",
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
    # "Polynomial Imputed Beds" = c("No", "Yes"),
    # "Polynomial Imputed Baths" = c("No", "Yes"),
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
  m0, m1, m3, m4, #m5,
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
  log_med_rent ~ share_units_evict + share_units_evict_sq | year,
  data = analytic_df[share_units_evict>0 & filing_rate < 1 & !is.na(baths_first_pred) & !is.na(share_units_evict)],
  cluster = ~PID
)

summary(m0_share)

m1_share <- fixest::feols(
  log_med_rent ~ share_units_evict+#share_units_evict_bins^2+
   i(num_units_bins, ref = "1") +
    #poly(log(num_units_evict),3)+
    #poly(log(num_units),3)+
    poly(number_stories,3) +
    poly(num_units_imp,3)+
    poly(year_built,3)
    #poly(beds_imp_first_pred,2) + poly(baths_first_pred,2)|
    |year^pm.zip +quality_grade_fixed,#+beds_imp_first_pred+baths_first_pred ,
 , data = bldg_panel[  filing_rate < 1 & share_units_evict> 0& !is.na(share_units_evict)],
  weights = ~num_units_imp,
  cluster = ~PID,
  combine.quick = F
)

summary(m1_share)

m1_share_alt <- fixest::feols(
  log_med_rent ~ filing_rate*# share_units_zip_sq+
    i(num_units_bins, ref = "1") +
    (hhi_unit_bins)+
    #log(num_units_zip)+
    #poly(log(num_units),2)+
    poly(number_stories,2) +
    poly(year_built,2)|
    #poly(beds_imp_first_pred,2) + poly(baths_first_pred,2)|
    GEOID^year+source +quality_grade_fixed ,
  , data = bldg_panel[source == "evict"&  filing_rate <= 1 & !is.na(share_units_evict)],
  weights = ~num_units_imp,
  cluster = ~PID,
  combine.quick = F
)

summary(m1_share_alt)


m3_share = fixest::feols(
  log_med_rent ~ share_units_zip +share_units_zip_sq| year,
  data = analytic_df[ filing_rate < 1 & !is.na(baths_first_pred) ],
  #weights = ~num_units,
  cluster = ~PID
)

m4_share = fixest::feols(
  log_med_rent ~ share_units_zip +share_units_zip_sq  + poly(year_built,2)+poly(number_stories,2) +
    poly(beds_imp_first_pred,2) + poly(baths_first_pred,2) +num_units_bins|
    GEOID^year+type_heater +quality_grade_fixed +
    view_type + source+
    general_construction ,
  data = analytic_df[filing_rate < 1& !is.na(baths_first_pred)],
  #weights = ~num_units,
  cluster = ~PID,
  combine.quick = F
)



bldg_panel[,high_filing := filing_rate > 0.15]
bldg_panel[,post_covid := year >= 2021]
bldg_panel[,placebo := year >= 2018 & year <= 2019]
bldg_panel[,high_filing_uniqueN := uniqueN(high_filing), by = PID]
bldg_panel[,.N, by = high_filing_uniqueN][order(high_filing_uniqueN)]
bldg_panel[,corp_owner := str_detect(owner, "CORP|LLC|INC|ASSOC|PARTNERSHIP|COMPANY|CORPORATION")]
(bldg_panel[,mean(med_price,na.rm=T), by = .(year,source)][order(source,year)][,yoy_change :=
                                                                         round(V1/data.table::shift(V1,1, type = "lag") - 1,2), by = .(source)][order(source,year)])
bldg_panel[,filing_rate_ntile := ntile(filing_rate, 5)]
pre_post_covid = bldg_panel[year %in% c(2018, 2019, 2022,2023),list(
  high_filing = first(high_filing),
  log_med_rent = mean(log_med_rent),
  num_units_bins = first(num_units_bins),
  num_units_imp = first(num_units_imp),
  filing_rate = first(filing_rate),
  corp_owner = first(corp_owner),
  year = max(year),
  filing_rate_ntile = first(filing_rate_ntile),
  num_years = uniqueN(year),
  GEOID = first(GEOID),
  CT_ID_10 = first(CT_ID_10)
  #cumsum_filing_rate = max(cumsum_filing_rate)
), by = .(PID, post_covid)]

pre_post_covid[,high_filing_uniqueN := uniqueN(high_filing), by = PID]
pre_post_covid[,.N, by = high_filing_uniqueN][order(high_filing_uniqueN)]
pre_post_covid[,high_filing_post_covid := high_filing * post_covid]
pre_post_covid[,filing_rate_ntile_post_covid := filing_rate_ntile * post_covid]
pre_post_covid[order(PID, year),change_log_rent := log_med_rent - lag(log_med_rent), by = .(PID)]

bldg_panel[,high_filing_post_covid := high_filing * post_covid]
bldg_panel[,filing_rate_post_covid := filing_rate * post_covid]
bldg_panel[,num_source := uniqueN(source), by = PID]
bldg_panel[,num_source_year := uniqueN(source), by = .(PID, year)]
bldg_panel[,num_corp_owner := uniqueN(corp_owner), by = PID]
m1 <- fixest::feols(
  log_med_rent ~high_filing_post_covid + num_units_bins * post_covid + corp_owner * post_covid
  |PID + year
  , data = bldg_panel[#source == "evict" &
    year %in% c(2018,2019,2022,2023) &
      filing_rate <=1  ],
  weights = ~(num_units_imp),
  cluster = ~PID,
  combine.quick = T
)

m2 <- fixest::feols(
  log_med_rent ~high_filing_post_covid + num_units_bins * post_covid + corp_owner * post_covid
  |PID + CT_ID_10^year
  , data = bldg_panel[#source == "evict" &
    year %in% c(2018,2019,2022,2023) &
      filing_rate <=1  ],
  weights = ~(num_units_imp),
  cluster = ~PID,
  combine.quick = T
)
summary(m2)

m3 <- fixest::feols(
  log_med_rent ~high_filing_post_covid + num_units_bins * post_covid + corp_owner * post_covid
  |PID + year
  , data = pre_post_covid[#source == "evict" &
    filing_rate <=1  ],
  weights = ~(num_units_imp),
  cluster = ~PID,
  combine.quick = T
)

m4 <- fixest::feols(
  log_med_rent ~high_filing_post_covid + num_units_bins * post_covid + corp_owner * post_covid
  |PID + CT_ID_10^year
  , data = pre_post_covid[#source == "evict" &
    filing_rate <=1  ],
  weights = ~(num_units_imp),
  cluster = ~PID,
  combine.quick = T
)

etable(m1,m2,m3,m4)

m5 <- feols(
  log_med_rent ~i(filing_rate_ntile_post_covid, ref = 1)  + num_units_bins * post_covid + corp_owner * post_covid
  |PID + year
  , data = pre_post_covid[#source == "evict" &
    filing_rate <=1  ],
  weights = ~(num_units_imp),
  cluster = ~PID,
  combine.quick = T
)

m6 <- feols(
  log_med_rent ~i(filing_rate_ntile_post_covid, ref = 1) + num_units_bins * post_covid + corp_owner * post_covid
  |PID + CT_ID_10^year
  , data = pre_post_covid[#source == "evict" &
    filing_rate <=1  ],
  weights = ~(num_units_imp),
  cluster = ~PID,
  combine.quick = T
)

etable(m3,m4,m5,m6, drop = c("corp_ownerTRUE$|num_units_bins[0-9+-]+$") )


# set default style
def_style = style.df(depvar.title = "", fixef.title = "",
                     fixef.suffix = " fixed effect", yesNo = c("Yes","No"))

fixest::setFixest_etable(
  digits = 4, fitstat = c("n")
)

setFixest_dict(
  c(
    "PID"= "parcel",
    "year" = "year",
    "high_filing_post_covid" = "High Evictors",
    "filing_rate_ntile_post_covid" = "Filing Rate Quintile",
    "log_med_rent" = "Log Median Rent",
    "num_units_imp" = "Number of Units",
    "change_log_rent" = "Change in Log Rent",
    "CT_ID_10" = "Census Tract",
    "filing_rate" = "Filing Rate",
    "filing_rate_ntile_post_covid" = "Filing Rate Quintile",
    "filing_rate_ntile_post_covid::2" = "Filing Rate 2nd Quintile",
    "filing_rate_ntile_post_covid::3" = "Filing Rate 3rd Quintile",
    "filing_rate_ntile_post_covid::4" = "Filing Rate 4th Quintile",
    "filing_rate_ntile_post_covid::5" = "Filing Rate 5th Quintile",
    'num_units_bins101+' = "100+ Units",
    'num_units_bins101+:post_covidTRUE' = "100+ Units (Post-COVID)",
    'num_units_bins51-100' = "51-100 Units",
    'num_units_bins51-100:post_covidTRUE' = "51-100 Units (Post-COVID)",
    'num_units_bins6-50' = "6-50 Units",
    'num_units_bins6-50:post_covidTRUE' = "6-50 Units (Post-COVID)",
    'num_units_bins2-5' = "2-5 Units",
    'num_units_bins2-5:post_covidTRUE' = "2-5 Units (Post-COVID)",
    'num_units_bins1' = "1 Unit",
    'num_units_bins1:post_covidTRUE' = "1 Unit (Post-COVID)",
    "post_covidTRUE" = "Post-COVID Period",
    "corp_ownerTRUE" = "Corporate Owner"


  )
)

header_evict = c("High Evictors",
                 "High Evictors (within Census Tract)",
                 "High Evictors (quintiles)",
                 "High Evictors (quintiles within Census Tract)")

evict_models <- list(#m3,m4,
                     m5,m6)


evict_tables = etable(
  evict_models,
  headers = header_evict[3:4],
  digits = 3,digits.stats = 3,
  keep = "%ost_covid",
  order = c("%high_filing_post_covid","%filing_rate_ntile_post_covid",
            "%num_units_bins","%corp_ownerTRUE","%post_covidTRUE"),
  #extralines = append("Wald Stat for Pre-Trends",owner_wald),
  title = "Price Change Regressions",
  drop = "cohort",
  tex = T)

evict_tables
writeLines(evict_tables, "tables/covid_price_change_regs.tex")

library(fwlplot)

fwlplot(
  change_log_rent ~filing_rate#*i(source,ref = "evict")
  | year
  , data = pre_post_covid[#source == "evict" &
    year %in% c(2022,2023) &
      # year <= 2019 | year >= 2022 &
      #year <= 2019 & year>=2014
      # &num_units_imp > 50
      #(last_obs == T) &
      filing_rate <=1  ],
  weights = ~(num_units_imp),
  cluster = ~PID,
  combine.quick = T
)


pre_post_covid[,filing_rate_round10 := round(filing_rate, 2) * 10]
pre_post_covid[,filing_rate_round5 := round(filing_rate * 20) / 20]
pre_post_covid_bin_scatter = pre_post_covid[filing_rate <= 1] %>%
  group_by(filing_rate_round5) %>%
  summarise(
    mean_change_log_rent = weighted.mean(change_log_rent, na.rm = TRUE, w= num_units_imp),
    n = n(),
    num_units = sum(num_units_imp, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  as.data.table()

pre_post_covid_bin_scatter[filing_rate_round5 <= 1 ] %>%
  ggplot(aes( x = filing_rate_round5, y = mean_change_log_rent)) +
  geom_point() +
  geom_smooth(method = "lm") +
  #facet_wrap(~year) +
  labs(
    title = "Change in log rent by filing rate",
    x = "Filing Rate",
    y = "Change in Log Rent"
  ) +
  theme_minimal()

placebo = bldg_panel[year %in% c(2018, 2019, 2017,2016),list(
  high_filing = first(high_filing),
  log_med_rent = mean(log_med_rent),
  num_units_imp = first(num_units_imp),
  filing_rate = first(filing_rate),
  year = max(year),
  filing_rate_ntile = first(filing_rate_ntile),
  num_years = uniqueN(year),
  GEOID = first(GEOID),
  CT_ID_10 = first(CT_ID_10)
  #cumsum_filing_rate = max(cumsum_filing_rate)
), by = .(PID, placebo)]

placebo[,high_filing_uniqueN := uniqueN(high_filing), by = PID]
placebo[,.N, by = high_filing_uniqueN][order(high_filing_uniqueN)]
placebo[,high_filing_post_placebo := high_filing * placebo]
placebo[,filing_rate_ntile_post_placebo := filing_rate_ntile * placebo]
placebo[order(PID, year),change_log_rent := log_med_rent - lag(log_med_rent), by = .(PID)]
m7 <- fixest::feols(
  log_med_rent ~high_filing_post_placebo#*i(source,ref = "evict")
  |PID + year
  , data = placebo[#source == "evict" &
    year %in% c(2018,2019,2017,2016) &
      filing_rate <=1  ],
  weights = ~(num_units_imp),
  cluster = ~PID,
  combine.quick = T
)

m8 <- fixest::feols(
  log_med_rent ~high_filing_post_placebo#*i(source,ref = "evict")
  |PID + CT_ID_10^year
  , data = placebo[#source == "evict" &
    year %in% c(2018,2019,2017,2016) &
      filing_rate <=1  ],
  weights = ~(num_units_imp),
  cluster = ~PID,
  combine.quick = T
)

etable(m7,m8)
setDT(philly_rentals_long)
philly_rentals_long = rent_list
rental_aggs = philly_rentals_long[,
                    list(
                       sum(numberofunits, na.rm = T),
                      sum(numberofunits[PID %in% bldg_panel[high_filing == T, PID]],na.rm =T)
                      ,.N,
                      sum(PID %in% bldg_panel[high_filing == T, PID],na.rm =T)
                      ),
                    by = year][year %in% 2015:2024][order(year)]

rental_aggs[,change_units_rel2019 := V1 / V1[year == 2019] - 1]
rental_aggs[,change_high_filing_units_rel2019 := V2 / V2[year == 2019] - 1]
rental_aggs[,change_parcels_rel2019 := N / N[year == 2019] - 1]
rental_aggs[,change_high_filing_parcels_rel2019 := V4 / V4[year == 2019] - 1]
# export as GT table
library(gt)
rental_aggs %>%
  filter(year >= 2017 & year <= 2023)%>%
  select(
    Year = year,
    `Total Units` = V1,
    `Units in High Evictor Buildings` = V2,
    `Total Rentals` = N,
    `Rentals in High Evictor Buildings` = V4,
    # `Change in Total Units Since 2019` = change_units_rel2019,
    # `Change in High Evictor Units Since 2019` = change_high_filing_units_rel2019,
    # `Change in Total Rentals Since 2019` = change_parcels_rel2019,
    # `Change in High Evictor Rentals Since 2019` = change_high_filing_parcels_rel2019
  ) %>%
  gt() %>%
  fmt_number(
    columns = c(`Total Units`, `Units in High Evictor Buildings`,
                `Total Rentals`, `Rentals in High Evictor Buildings`,

                ),
    decimals = 0,
    use_seps = TRUE
  ) %>%
  # fmt_percent(
  #   columns = c(`Change in Total Rentals Since 2019`, `Change in High Evictor Rentals Since 2019`,
  #               `Change in Total Units Since 2019`, `Change in High Evictor Units Since 2019`),
  #   decimals = 1
  # ) %>%
  tab_header(
    title = "Philadelphia Rental Registry: Number of Units Registered",
    subtitle = "2017-2023"
  ) %>%
  tab_source_note(
    source_note = "Data from Philadelphia Housing Rental Registry and Eviction Lab"
  ) %>%
  as_latex()%>%
  write_lines("tables/rental_stock_over_time.tex")

#### graveyard ####
fixest::feols(violations_per_unit ~ filing_rate,
              # offset = ~num_units_imp,
              data = bldg_panel[year == 2018])


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
  log_med_rent ~ 1|year,
  data = analytic_df[ filing_rate < 1],
  # weights = ~num_units,
  cluster = ~PID
)

analytic_df[,filings_year_resid := filing_rate - predict(year_resid_filing_reg, newdata = analytic_df) ]
analytic_df[,prices_year_resid := log_med_rent - predict(year_resid_price_reg, newdata = analytic_df) ]

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
  log_med_rent ~ poly(num_units,2) + poly(year_built,2)+poly(number_stories,2) +
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
analytic_df[,prices_prop_resid := log_med_rent - predict(price_resid_reg, newdata = analytic_df) ]

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
  log_med_rent ~
    +filing_rate |
    GEOID+year  ,
  data = analytic_df[filing_rate<=2 & source == "evict" ]
)

feols(
  log_med_rent ~
    +filing_rate |
    GEOID^year  ,
  data = analytic_df[filing_rate<=2 & source == "evict" ]
)


fwlplot::fwlplot(
  log_med_rent ~
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
  geom_density(data=pa_tract[GEOID %in% analytic_df$CT_ID_10] %>% mutate(med_price = gross_rentE, source ="Census Tract Median" ),
               aes( color = "Census Tract Median"))+
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
  log_med_rent ~ share_units_evict + share_units_evict_sq |
    year^GEOID+source  + quality_grade_fixed
  , data = analytic_df[
    num_units > 20 &
      source == "evict"
    # filing_rate <1 & filing_rate >0.2 & share_units_evict >0 &
    #   share_units_evict < 1 & !is.na(baths_first_pred) & !is.na(share_units_evict)
  ]
)


# bin scatter
#put filing rate to 0.05 buckets
bldg_panel[, filing_rate_round := round(filing_rate, 2)]

bldg_panel_bins <- bldg_panel[ filing_rate <= 1,list(
  mean_med_rent = weighted.mean(med_price,w = num_units_imp, na.rm = T),
  .N
), by = .(filing_rate = round(1000*filing_rate / 25) * 25, source)]

ggplot(bldg_panel_bins, aes(color = source,x = round(filing_rate, 2), y = mean_med_rent)) +
  geom_point() +
  geom_smooth(se = F) +
  labs(
    title = "Mean Rent by Filing Rate",
    subtitle = "Philadelphia, 2018",
    x = "Filing Rate",
    y = "Mean Rent"
  ) +
  theme_philly_evict()
