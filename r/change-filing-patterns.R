# eviction changes

library(data.table)
library(tidyverse)
library(sf)
library(tidycensus)
library(fixest)

source('r/helper-functions.R')
indir = "/Users/joefish/Desktop/data/philly-evict/processed"
analytic_df = fread(file.path(indir, "analytic_df.csv"))
bldg_panel = fread(file.path(indir, "bldg_panel.csv"))
rent_list = fread(file.path(indir, "license_long_min.csv"))
parcels = fread(file.path(indir, "parcel_building_2024.csv"))

# designate high-filing by pre-covid filing average
bldg_panel2019 = bldg_panel[year < 2019, list(
  sum_filings = sum(num_filings, na.rm=TRUE),
  num_units_imp = first(num_units_imp),
  num_units_bins = first(num_units_bins),
  num_years = 2019 - 2006 + 1 # eviction data starts in 2006
), by = PID]
bldg_panel2019[, filing_rate := sum_filings / num_years / num_units_imp]
bldg_panel2019[,high_filing := ifelse(filing_rate > 0.15, 1, 0)]

# repeat for post 2022
bldg_panel2022 = bldg_panel[year >= 2022 & year <= 2024, list(
  sum_filings = sum(num_filings, na.rm=TRUE),
  num_units_imp = first(num_units_imp),
  num_units_bins = first(num_units_bins),
  num_years = 2024 - 2021 + 1 # eviction data starts in 2006
), by = PID]

bldg_panel2022[, filing_rate := sum_filings / num_years / num_units_imp]
bldg_panel2022[,high_filing := ifelse(filing_rate > 0.15, 1, 0)]

bldg_panel = bldg_panel %>%
  select(-matches('2019|2022|2021')) %>%
  merge(bldg_panel2019[, .(PID, high_filing_2019 = high_filing, filing_rate2019 = filing_rate)], by = "PID", all.x = TRUE) %>%
  merge(bldg_panel2022[, .(PID, high_filing_2022 = high_filing, filing_rate2022 = filing_rate)], by = "PID", all.x = TRUE)

bldg_panel[,high_filing_2019 := replace_na(high_filing_2019,FALSE)]
bldg_panel[,high_filing_2022 := replace_na(high_filing_2022,FALSE)]
bldg_panel[,filing_rate_cuts := cut(
  filing_rate,
  breaks = c(-Inf,  0.1, 0.2, 0.3, Inf),
  labels = c("0-10%",  "10-20%", "20-30%", "30%+"),
  include.lowest = TRUE
) ]

bldg_panel[,filing_rate_cuts_2019 := cut(
  filing_rate2019,
  breaks = c(-Inf,  0.1, 0.2, 0.3, Inf),
  labels = c("0-10%",  "10-20%", "20-30%", "30%+"),
  include.lowest = TRUE
) ]

bldg_panel[,postCOVID := ifelse(year >= 2020, 1, 0)]
bldg_panel[,num_years_in_sample := .N, by = PID]
bldg_panel[,num_years_in_sample_post15 := sum(year > 2014), by = PID]
bldg_panel[,high_filing := filing_rate > 0.2]
bldg_panel[,high_filing_2019_postCOVID := high_filing_2019 * postCOVID]
bldg_panel[,exists_pre_post_COVID := min(year ) < 2020 & max(year ) > 2021, by = PID]
bldg_panel[,change_rent := log_med_rent - lag(log_med_rent), by = PID]
bldg_panel[,change_year := year - data.table::shift(year), by = PID]
bldg_panel[,change_rent_annualized := change_rent / change_year]
bldg_panel[,max_change_rent_annualized := max(change_rent_annualized, na.rm=TRUE), by = PID]
bldg_panel[,quantile(change_rent_annualized, probs = seq(0,1, by=0.1), na.rm=TRUE)]
bldg_panel[,num_sources:=uniqueN(source), by = PID]


es_filing = feols(log_med_rent ~ high_filing_2019*i(year,ref = 2019)|PID+year   ,
                 weights = ~num_units_imp,
                  data = bldg_panel[
                   year %in% 2011:2023  & max_change_rent_annualized <= 0.5
                                 ])
summary(es_filing, cluster = "PID")

make_es_plot <- function(model,ref_year=2018){
  # extract model using broom::tidy
  # make event study coefficients
  # add a coefficient of 1 for the reference year
  es_coefs = broom::tidy(model) %>%
    filter(str_detect(term, "high_filing_.+:year")) %>%
    mutate(
      year = as.integer(str_extract(term, "[0-9]{4}$")),
      conf_low = estimate - 1.96*std.error,
      conf_high = estimate + 1.96*std.error
    ) %>%
    bind_rows(tibble(
      term = "Reference Year",
      year = ref_year,
      estimate = 0,
      std.error = 0,
      conf_low = 0,
      conf_high = 0
    )) %>%
    arrange(year)
  print(es_coefs)
  # plot
  ggplot(es_coefs, aes(x=year, y=estimate)) +
    geom_point() +
    geom_errorbar(aes(ymin=conf_low, ymax=conf_high)) +
    geom_hline(yintercept = 0, linetype="dashed", color = "red") +
    theme_philly_evict() +
    labs(
      title = "Event Study: High Filing Rate Buildings vs Low Filing Rate Buildings",
      subtitle = paste0("High filing rate buildings defined as those with pre-COVID filing rate > 10%.\nReference year = ", ref_year),
      caption = "Data: Philadelphia eviction filings and rental licenses",
      x = "Year",
      y = "Estimated Coefficient (log points)"
    ) +
    scale_x_continuous(breaks = seq(min(es_coefs$year), 2023, by=1)) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 1, suffix = " pp"), breaks = seq(-1,1,0.05)) +
    theme(
      plot.title = element_text(size=16, face="bold"),
      plot.subtitle = element_text(size=12),
      plot.caption = element_text(size=8)
    )
}

make_es_plot(es_filing, ref_year = 2019)
ggsave("figs/event_study_high_filing_2019.png", width=10, height=10, bg = "white")

# repeat but add controls for neighborhood and building characteristics
es_filing_controls = feols(log_med_rent ~ high_filing_2019*i(year,ref = 2019)
                           |PID +CT_ID_10^year  ,
                            weights = ~num_units_imp,
                           cluser = ~PID,
                            data = bldg_panel[
                              year %in% 2011:2023 & num_units_imp <= 500 & max_change_rent_annualized <= 0.5
                            ])

summary(es_filing_controls, cluster = "PID")
make_es_plot(es_filing_controls, ref_year = 2019)
ggsave("figs/event_study_high_filing_2019_nhood_trends.png", width=10, height=10, bg = "white")

filing_rates_pre_post_covid = bldg_panel[filing_rate <= 1,list(
  pre_covid = mean(filing_rate2019[year < 2020], na.rm=TRUE),
  post_covid = mean(filing_rate2022[year > 2021], na.rm=TRUE),
  num_units_imp = first(num_units_imp)
), by = PID]

filing_rates_pre_post_covid[,pre_covid := replace_na(pre_covid, 0)]
filing_rates_pre_post_covid[,post_covid := replace_na(post_covid, 0)]

ggplot(filing_rates_pre_post_covid[num_units_imp >= 25 & pre_covid < 0.75 & post_covid < 0.75 ],
       aes(x=pre_covid, y=post_covid) ) +
  geom_point(alpha=0.5) +
  geom_smooth( color='blue', se=T)+
  geom_abline(slope=1, intercept = 0, color='red') +
  theme_philly_evict() +
  labs(
    title = "Eviction Filing Rates: Pre-COVID vs Post-COVID",
    subtitle = "Post-COVID = 2022-2024; Pre-COVID = 2006-2019",
    caption = "Each point is a building with 25 or more units. The red line indicates no change in filing rate",
    x = "Pre-COVID Filing Rate",
    y = "Post-COVID Filing Rate"
  )
ggsave("figs/filing_rate_pre_post_covid.png", width=10, height=10, bg = "white")


