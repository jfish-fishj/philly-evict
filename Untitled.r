bldg_panel = fread("~/Desktop/data/philly-evict/processed/bldg_panel.csv")
parcel_bldg = fread("~/Desktop/data/philly-evict/processed/parcel_building.csv")
infousa = fread("~/Desktop/data/philly-evict/processed/infousa_parcel_occupancy_vars.csv")
rentals = fread("~/Desktop/data/philly-evict/processed/license_long_min.csv")
ev_pid_year = fread("~/Desktop/data/philly-evict/processed/evict_pid_year.csv")
parcel_bldg[,pre_covid := year < 2020]
parcel_bldg[,post_covid := year >= 2020]
parcel_bldg[,PID := as.numeric(PID)]
parcel_bldg_rentals = parcel_bldg[(PID %in% bldg_panel$PID | PID %in% rentals$PID) & year %in% 2007:2024] %>%
  select(-c(num_units_imp)) %>%
  merge(ev_pid_year, by = c("PID", "year"), all.x = T) %>%
  merge(infousa %>% select(-c(market_value,num_units,year_built,total_area)),
        by = c("PID", "year"),
        all.x = T)

parcel_bldg_rentals = parcel_bldg_rentals[!is.na(num_units_imp)]
parcel_bldg_rentals[,num_filings_total_preCOVID := replace_na(num_filings_total_preCOVID,0)]
parcel_bldg_rentals[,filing_rate_preCOVID := num_filings_total_preCOVID / num_units_imp /13, by = PID]
parcel_bldg_rentals[,high_filing := filing_rate_preCOVID > 0.1]
parcel_bldg_rentals[,sum(is.na(high_filing),na.rm =T), by = year]

parcel_bldg_agg = parcel_bldg_rentals %>%
  filter(num_units_imp > 5) %>%
  group_by(post_covid,high_filing,filing_rate_preCOVID,year_built, PID,total_livable_area, num_units_imp) %>%
  summarise(across(total_permits:other_investigation_count, ~ sum(.x,na.rm = T) / n() ),
            market_value = mean(market_value) ) %>%
  ungroup()
setDT(parcel_bldg_agg)
colnames(parcel_bldg_agg)
# cut into 6-50; 50+
parcel_bldg_rentals[,num_units_cuts := cut(num_units_imp, breaks = c(5,50,1000), labels = c("6-50","50+"))]

# cut into 6-9; 10-24; 25-49; 50-99, 100+
parcel_bldg_rentals[,num_units_cuts_granular := cut(num_units_imp, breaks = c(5,9,24,49,99,Inf), labels = c("6-9","10-24","25-49","50-99","100+"))]
parcel_bldg_rentals[,year_built_decade := floor(year_built / 10) * 10]
parcel_bldg_rentals[,market_value_per_unit := market_value / num_units_imp]
parcel_bldg_rentals[,market_value_per_sqft := market_value / total_livable_area]

parcel_bldg_rentals[post_covid == F,above_median_market_value_preCOVID := market_value_per_unit > median(market_value_per_unit, na.rm = T),
                by = .(high_filing,  num_units_cuts_granular)]



# repeat psf
parcel_bldg_rentals[,market_value_pre_covid := max(market_value[post_covid == F], na.rm = T), by = PID]
parcel_bldg_rentals[,market_value_per_sqft_pre_covid := market_value_pre_covid / total_livable_area]

parcel_bldg_rentals[post_covid == F,above_median_market_value_per_sqft_preCOVID := market_value_per_sqft > median(market_value_per_sqft, na.rm = T),
                by = .(high_filing,  num_units_cuts_granular)]

# get the 2x2 matrix of each value
parcel_bldg_rentals[!is.na(above_median_market_value_per_sqft_preCOVID),.N,
                by = .(above_median_market_value_preCOVID, above_median_market_value_per_sqft_preCOVID)][, per := N / sum(N)][]

# fill in for post_covid
parcel_bldg_rentals[,above_median_market_value_preCOVID := max(above_median_market_value_preCOVID,na.rm =T), by = PID]
parcel_bldg_rentals[,above_median_market_value_per_sqft_preCOVID := max(above_median_market_value_per_sqft_preCOVID,na.rm =T), by = PID]

parcel_bldg_rentals[,old_building := year_built < quantile(year_built, w= num_units_imp, probs = c(0.25),na.rm =T), by = high_filing]

parcel_bldg_rentals[,quantile(total_permits)]
parcel_bldg_rentals[,permitted := total_permits > 0]
parcel_bldg_rentals[,building_permitted := building_permit_count > 0]
parcel_bldg_rentals[,violated := total_violations > 0]
parcel_bldg_rentals[,unsafe_hazordous_dangerous_violated := (imminently_dangerous_violation_count + hazardous_violation_count + unsafe_violation_count) > 0]
parcel_bldg_rentals[,complained := total_complaints > 0]
parcel_bldg_rentals[,residential_building_permitted := residential_building_permit_count > 0]
parcel_bldg_rentals[,mechanical_permitted := mechanical_permit_count > 0]
parcel_bldg_rentals[,electrical_permitted := electrical_permit_count > 0]
parcel_bldg_rentals[,mechanical_or_electrical_permitted := (mechanical_permit_count + electrical_permit_count) > 0]
parcel_bldg_rentals[num_units_imp > 5,list(
  mean(permitted,na.rm =T),
  mean(building_permitted,na.rm =T),
  mean(residential_building_permitted,na.rm =T),
  mean(mechanical_permitted,na.rm =T),
  mean(electrical_permitted,na.rm =T),
  mean(mechanical_or_electrical_permitted,na.rm =T),
  mean(violated,na.rm =T),
  mean(unsafe_hazordous_dangerous_violated,na.rm =T),
  mean(complained,na.rm =T)

), by = post_covid]
# twfe models for pre / post COVID
library(fixest)
# model1 = feglm(permitted ~ post_covid * high_filing * old_building,
#                 data = parcel_bldg_agg[],
#                 family = "logit",
#                 weights = ~num_units_imp,
#                 cluster = ~PID)
# model1
model1 = feols(building_permitted ~ high_filing : i(year, ref = 2019)|high_filing + year, #+ log(total_livable_area)+log(market_value_per_sqft_pre_covid) |num_units_cuts_granular+year_built_decade,
               ,weights = ~num_units_imp,
               #family = "logit",
               data = parcel_bldg_rentals[num_units_imp > 5 & year %in% 2015:2023 & !is.na(high_filing)], cluster = ~PID)

model1 %>% summary()


model2 = feglm(building_permitted ~ post_covid * high_filing * old_building#+ log(total_livable_area)+log(market_value_per_sqft_pre_covid) |num_units_cuts_granular+year_built_decade,
               #,weights = ~num_units_imp
               ,family = "logit",
                data = parcel_bldg_rentals, cluster = ~PID)
model2

model3 = feols(mechanical_permitted ~ high_filing : i(year, ref = 2019)|high_filing + year
              , weights = ~num_units_imp
              #,family = "logit"
                               ,data = parcel_bldg_rentals[num_units_imp > 5], cluster = ~PID)
model3

model4 = feglm(mechanical_permitted ~post_covid * high_filing * old_building
              , weights = ~num_units_imp
               ,family = "logit",
               data = parcel_bldg_rentals[num_units_imp > 5], cluster = ~PID)
model4

model5 = feglm(mechanical_or_electrical_permitted ~ post_covid * high_filing,# + log(total_livable_area)+log(market_value_per_sqft_pre_covid) |num_units_cuts_granular+year_built_decade
              , weights = ~num_units_imp
              ,family = "logit",
                data = parcel_bldg_agg[], cluster = ~PID)
model5

model6 = feglm(mechanical_or_electrical_permitted~ high_filing : i(year, ref = 2019)|high_filing + year,# + log(total_livable_area)+log(market_value_per_sqft_pre_covid) |num_units_cuts_granular+year_built_decade
               , weights = ~num_units_imp
               ,family = "logit",
               data = parcel_bldg_rentals[], cluster = ~PID)
model6




# violations models
model4 = feglm(unsafe_hazordous_dangerous_violated ~  post_covid * high_filing  #+ log(total_livable_area) |num_units_cuts_granular,
              , data = parcel_bldg_agg,
                weights = ~num_units_imp,
                family = "logit",
                cluster = ~PID)
model4

model5 = feols(unsafe_hazordous_dangerous_violated ~ high_filing : i(year, ref = 2019)|high_filing + year,  #+log(market_value_per_sqft_pre_covid) + log(total_livable_area) +num_units_cuts_granular,
              , data = parcel_bldg_rentals[num_units_imp > 5],
              #  family = "logit",
             , weights = ~num_units_imp,
                cluster = ~PID)
model5 %>% coefplot()

#library(marginaleffects)
marginaleffects::avg_comparisons(model5,variables = c("high_filing", "post_covid"),
                                 # newdata = datagrid(high_filing = c(TRUE, FALSE),
                                 #                    post_covid = c(TRUE, FALSE)),
                                 cross = T)

model6 = feols(violated ~  post_covid * high_filing  ,
               data = parcel_bldg_agg,
                weights = ~num_units_imp,
                cluster = ~PID)
model6

avg_comparisons(model6,variables = c("high_filing", "post_covid"), cross = T)

model7 = feols(complained ~  post_covid * high_filing,
               data = parcel_bldg_agg,
                weights = ~num_units_imp,
                cluster = ~PID)
model7

avg_comparisons(model7,variables = c("high_filing", "post_covid"), cross = T)

parcel_bldg_rentals[,severe_complain := (fire_complaint_count  + heat_complaint_count ) > 0]
parcel_bldg_rentals[,num_units_cuts := case_when(
  num_units_imp == 1 ~ "1",
  num_units_imp >= 2 & num_units_imp <= 5 ~ "2-5",
  num_units_imp >= 6 & num_units_imp <= 50 ~ "6-50",
  num_units_imp > 50 ~ "50+",
  TRUE ~ NA_character_
)]
time_aggs = parcel_bldg_rentals %>%
  filter(num_units_imp > 1) %>%
  group_by(year, high_filing, num_units_cuts) %>%
  summarise(across(matches("permit|violate|complaint"),
                   list(weighted_mean = ~ weighted.mean(.x > 0, w = num_units_imp, na.rm = T),
                        unweighted_mean = ~ mean(.x > 0, na.rm = T) ), .names = "{.fn}_{.col}"),
            N = n(),
            total_units = sum(num_units_imp, na.rm = T) ) %>%
  ungroup() %>%
  arrange(year, high_filing, num_units_cuts) %>%
  group_by(high_filing, num_units_cuts) %>%
  mutate(across(contains("weighted_mean"), ~ .x - .x[year == 2019] - 1, .names="rel_{.col}") ) %>%
  ungroup()

setDT(time_aggs)

# time_aggs = parcel_bldg_rentals[num_units_imp > 5,list(
#   weighted_mean_permit = weighted.mean(building_permitted, w = num_units_imp, na.rm = T),
#   weighted_mean_violation = weighted.mean(violated, w = num_units_imp, na.rm = T),
#   weighted_mean_complaint = weighted.mean(complained, w = num_units_imp, na.rm = T ),
#   weighted_mean_building_permit = weighted.mean(building_permitted, w = num_units_imp, na.rm = T),
#   weighted_mean_mechanical_permit = weighted.mean(mechanical_permitted, w = num_units_imp, na.rm = T),
#   weighted_mean_electrical_permit = weighted.mean(electrical_permitted, w = num_units_imp, na.rm = T),
#   weighted_mean_mechanical_or_electrical_permit = weighted.mean(mechanical_or_electrical_permitted, w = num_units_imp, na.rm = T),
#   weighted_mean_unsafe_hazordous_dangerous_violated = weighted.mean(unsafe_hazordous_dangerous_violated, w = num_units_imp, na.rm = T),
#   weighted_mean_structural_deficiency_complaint = weighted.mean(structural_deficiency_complaint_count >0, w = num_units_imp, na.rm = T),
#   weighted_mean_severe_complain = weighted.mean(severe_complain, w = num_units_imp, na.rm = T),
#   unweighted_mean_permit = mean(building_permitted, na.rm = T),
#   unweighted_mean_violation = mean(violated, na.rm = T),
#   unweighted_mean_complaint = mean(complained, na.rm = T ),
#   unweighted_mean_building_permit = mean(building_permitted, na.rm = T),
#   unweighted_mean_mechanical_permit = mean(mechanical_permitted, na.rm = T),
#   unweighted_mean_electrical_permit = mean(electrical_permitted, na.rm = T),
#   unweighted_mean_mechanical_or_electrical_permit = mean(mechanical_or_electrical_permitted, na.rm = T),
#   unweighted_mean_unsafe_hazordous_dangerous_violated = mean(unsafe_hazordous_dangerous_violated, na.rm = T),
#   unweighted_mean_severe_complain = mean(severe_complain, na.rm = T),
#   unweighted_mean_structural_deficiency_complaint = mean(structural_deficiency_complaint_count >0, na.rm = T),
#   N = .N,
#   total_units = sum(num_units_imp, na.rm = T)
# ), by = .(year, high_filing, num_units_cuts)]

time_aggs = time_aggs %>%
  group_by(high_filing, num_units_cuts) %>%
  mutate(across(contains("weighted"), ~ .x - .x[year == 2019] - 1, .names="rel_{.col}") ) %>%
  ungroup()
setDT(time_aggs)

ggplot(time_aggs, aes(x = year, y = unweighted_mean_permit, color = high_filing)) +
  geom_line() +
  geom_point() +
  facet_wrap(~num_units_cuts) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
  labs(title = "Relative Change in Building Permit Rates Over Time by Filing History",
       x = "Year",
       y = "Relative Change in Building Permit Rate",
       color = "High Filing History") +
  theme_minimal()

ggplot(time_aggs, aes(x = year, y = unweighted_mean_mechanical_or_electrical_permit, color = high_filing)) +
  geom_line() +
  geom_point() +
  facet_wrap(~num_units_cuts) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
  labs(title = "Relative Change in Mechanical or Electrical Permit Rates Over Time by Filing History",
       x = "Year",
       y = "Relative Change in Mechanical or Electrical Permit Rate",
       color = "High Filing History") +
  theme_minimal()

ggplot(time_aggs, aes(x = year, y = unweighted_mean_unsafe_hazordous_dangerous_violated, color = high_filing)) +
  geom_line() +
  geom_point() +
  facet_wrap(~num_units_cuts) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
  labs(title = "Relative Change in Unsafe/Hazardous/Dangerous Violation Rates Over Time by Filing History",
       x = "Year",
       y = "Relative Change in Unsafe/Hazardous/Dangerous Violation Rate",
       color = "High Filing History") +
  theme_minimal()

ggplot(time_aggs, aes(x = year, y = unweighted_mean_property_maintenance_complaint_count, color = high_filing)) +
  geom_line() +
  geom_point() +
  facet_wrap(~num_units_cuts) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
  labs(title = "Relative Change in Complaint Rates Over Time by Filing History",
       x = "Year",
       y = "Relative Change in Complaint Rate",
       color = "High Filing History") +
  theme_minimal()


# density of filing rates
parcel_aggs = parcel_bldg_rentals %>% distinct(PID, num_units_imp,filing_rate_preCOVID, .keep_all = T )

setDT(parcel_aggs)

# empirical cdf of filing rates; weighted by num units
parce_aggs_uncount <- parcel_aggs[!is.na(filing_rate_preCOVID) &
                                    !is.infinite(filing_rate_preCOVID) &
                                    filing_rate_preCOVID >=0 &
                                    filing_rate_preCOVID <=1] %>%
  uncount(weights = num_units_imp,.remove = F)

setDT(parce_aggs_uncount)
ggplot(parce_aggs_uncount, aes(x = filing_rate_preCOVID)) +
  stat_ecdf(size = 1) +
  geom_vline(aes(xintercept = mean(parce_aggs_uncount$filing_rate_preCOVID,na.rm=T),
                 color = "Mean Filing Rate"), linetype = "dashed") +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1), breaks = seq(0,1,0.2)) +
  scale_x_continuous(labels = scales::percent_format(accuracy = 1), limits = c(0, 0.5)) +
  # label the line as the mean filing rate
  annotate(
    "text",
    x = mean(parce_aggs_uncount$filing_rate_preCOVID,na.rm=T) + 0.02,
    y = 0.1,
    label = paste0("Mean Filing Rate: ", round(mean(parce_aggs_uncount$filing_rate_preCOVID,na.rm=T) * 100, 2), "%"),
  #  color = "Mean Filing Rate",
    hjust = 0,
    size = 5
  ) +
  labs(title = "Empirical CDF of Pre-COVID Filing Rates",
       x = "Filing Rate (Pre-COVID)",
       y = "Empirical CDF",
       color = "") +
  theme_philly_evict() +guides(color = "none")

ggsave("/Users/joefish/Documents/GitHub/philly-evictions/figs/empirical_cdf_filing_rate_preCOVID.png",
       width = 8, height = 6, bg = "white")

# order number of units factor by number of units levels are 1;2-5; 6-50; 50+
parce_aggs_uncount[,num_units_bins := cut(num_units_imp, breaks = c(1,2,5,50,Inf),
                                          labels = c("1","2-5","6-50","50+"),
                                          include.lowest = T,
                                          ordered_result = T)]



ggplot(parce_aggs_uncount, aes(x = filing_rate_preCOVID, group = num_units_bins, color = num_units_bins)) +
  stat_ecdf(size = 1) +
  scale_x_continuous(labels = scales::percent_format(accuracy = 1), limits = c(0, 0.5)) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1), breaks = seq(0,1,0.2)) +
  labs(title = "Empirical CDF of Pre-COVID Filing Rates",
       subtitle = "By Number of Units",
       x = "Filing Rate (Pre-COVID)",
       y = "Empirical CDF",
       color = "High Filing History") +
  theme_philly_evict() +
  viridis::scale_color_viridis(discrete = T, option = "H") +
  facet_wrap(~num_units_bins) +
  # add mean filing rate by group
  geom_vline(data = parce_aggs_uncount[,.(mean_filing_rate = mean(filing_rate_preCOVID, na.rm = T)), by = num_units_bins],
             aes(xintercept = mean_filing_rate, color = num_units_bins),
             linetype = "dashed") +
  guides(color = "none") +

    # label the line as the mean filing rate
  geom_text(data = parce_aggs_uncount[,.(mean_filing_rate = mean(filing_rate_preCOVID, na.rm = T)), by = num_units_bins],
            aes(x = mean_filing_rate + 0.02, y = 0.1,
                label = paste0("Mean: ", round(mean_filing_rate * 100, 2), "%")),
            hjust = 0,
            size = 4)
ggsave("/Users/joefish/Documents/GitHub/philly-evictions/figs/empirical_cdf_filing_rate_preCOVID_by_num_units.png",
       width = 10, height = 8, bg = "white")

evict_list$ev_pid_year[year %in% 2007:2023,sum(num_filings,na.rm=T), by = year] %>%
  ggplot(aes(x = year, y = V1)) +
  geom_line(size = 1, color = "blue") +
  geom_point(size = 2, color = "blue") +
  scale_x_continuous(breaks = seq(2007, 2024, 1)) +
  scale_y_continuous(labels = scales::comma_format()) +
  labs(title = "Number of Eviction Filings in Philadelphia by Year",
       x = "Year",
       y = "Number of Filings") +
  theme_philly_evict()

ggsave("/Users/joefish/Documents/GitHub/philly-evictions/figs/num_eviction_filings_by_year.png",
       width = 8, height = 6, bg = "white")




ev_parcel = evict_list$ev_pid_year
ev_parcel[,num_filings := replace_na(num_filings,0)]

ev_parcel[,pre_COVID := year < 2020]
ev_parcel[,post_COVID := year >= 2020]

ev_parcel_aggs = ev_parcel[year %in% 2007:2019 | year %in% c(2022,2023),list(
  mean_filings = mean(num_filings,na.rm=T)
), by = .(PID, post_COVID)] %>%
  # turn to wider
  pivot_wider(id_cols = PID,names_from = post_COVID, values_from = mean_filings, names_prefix = "post_COVID_") %>%
  mutate(change_in_filings = post_COVID_TRUE - post_COVID_FALSE)
setDT(ev_parcel_aggs)
ev_parcel_aggs[,list(
  sum(post_COVID_FALSE,na.rm=T),
  sum(post_COVID_TRUE,na.rm=T)
)]

ev_parcel_aggs = ev_parcel_aggs %>%
  merge(parcel_bldg[year == 2022,.(num_units, PID, num_units_imp, year_built,
                                   building_code_description_new_fixed,
                                   quality_grade_fixed)], by = "PID", all.x =T)

setDT(ev_parcel_aggs)
ev_parcel_aggs[,mean(is.na(num_units_imp))]
ev_parcel_aggs[,num_units_bins := cut(num_units_imp, breaks = c(1,2,5,50,Inf),
                                          labels = c("1","2-5","6-50","50+"),
                                          include.lowest = T,
                                          ordered_result = T)]

ev_parcel_aggs[,filing_rate_preCOVID := post_COVID_FALSE / num_units_imp ]
ev_parcel_aggs[,filing_rate_postCOVID := post_COVID_TRUE / num_units_imp ]


ggplot(ev_parcel_aggs[ !is.na(change_in_filings)],
       aes(y = post_COVID_TRUE, x =post_COVID_FALSE)) +
  geom_point(alpha = 0.25) +
  geom_smooth()+
  geom_smooth(method = "lm", se = F, color = "orange") +
  # label slope
  annotate("text", x = 100, y = 50,
           label = paste0("Slope: ", round(lm(post_COVID_TRUE ~ post_COVID_FALSE,
                                              data = ev_parcel_aggs[!is.na(change_in_filings)])$coefficients[2],2)),
           color = "orange") +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "red") +
  scale_x_continuous(labels = scales::comma_format(), breaks = seq(0,250,25)) +
  scale_y_continuous(labels = scales::comma_format(), breaks = seq(0,150,25)) +
  #scale_x_continuous(breaks = seq(-10, 10, 1), limits = c(-10, 10)) +
  #scale_y_continuous(labels = scales::comma_format()) +
  labs(title = "Change in Average Annual Eviction Filings Pre- and Post-COVID",
       #subtitle = "For Properties with More Than 5 Units",
       x = "Average Annual Filings Pre-COVID",
       y = "Average Annual Filings Post-COVID"
       ) +
  #facet_wrap(~num_units_bins)+
  theme_philly_evict()

ggsave("/Users/joefish/Documents/GitHub/philly-evictions/figs/change_in_avg_annual_eviction_filings_pre_post_COVID.png",
       width = 8, height = 6, bg = "white")


# repeat but filing rates
ggplot(ev_parcel_aggs[num_units_imp > 10 & !is.na(change_in_filings) & !is.infinite(filing_rate_preCOVID) & !is.infinite(filing_rate_postCOVID)],
       aes(y = filing_rate_postCOVID, x =filing_rate_preCOVID)) +
  geom_point(alpha = 0.25) +
  geom_smooth()+
  geom_smooth(method = "lm", se = F, color = "orange") +
  # label slope
  annotate("text", x = 0.45, y = 0.3,

           label = paste0("Slope: ", round(lm(filing_rate_postCOVID ~ filing_rate_preCOVID,
                                              data = ev_parcel_aggs[!is.na(change_in_filings) & !is.infinite(filing_rate_preCOVID) & !is.infinite(filing_rate_postCOVID)])$coefficients[2],2)),
           color = "orange") +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "red") +
  scale_x_continuous(labels = scales::percent_format(accuracy = 1), breaks = seq(0,0.5,0.05), limits = c(0,0.5)) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1), breaks = seq(0,0.5,0.05), limits = c(0,0.5)) +
  #scale_x_continuous(breaks = seq(-10, 10, 1), limits = c(-10, 10)) +
  #scale_y_continuous(labels = scales::comma_format()) +
  labs(title = "Change in Average Annual Eviction Filing Rates Pre- and Post-COVID",
       #subtitle = "For Properties with More Than 5 Units",
       x = "Average Annual Filing Rate Pre-COVID",
       y = "Average Annual Filing Rate Post-COVID"
  ) +
  #facet_wrap(~num_units_bins)+
  theme_philly_evict()




