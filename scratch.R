library(data.table)
library(tidyverse)
library(sf)
library(tidycensus)

philly_hist = readRDS("/Users/joefish/Desktop/data/philly-evict/philadelphia_historical.rds")
philly_cur = readRDS("/Users/joefish/Desktop/data/philly-evict/philadelphia_2020_2021.rds")

setDT(philly_hist)
setDT(philly_cur)

philly_census = get_acs(
  year= 2015,
  survey = "acs5",
  variables = c(
    "pop"= "B01001_001",
    "med_rent" = "B25064_001",
    #"asian"  = "B02001_005",
    "med_age" = "B01002_001",
    "hh"="B25003_001",
    "renter_hh" = "B25003_003",
    "owner_hh"="B25003_002",
    "medhhinc" = "B19013_001",
    "medhhinc_owner"="B25119_002",
    "medhhinc_renter" = "B25119_003",
    "gr_less_10_percent" = "B25070_002",
    "gr_10_15_percent" = "B25070_003",
    "gr_16_20_percent" = "B25070_004",
    "gr_21_15_percent" = "B25070_005",
    "gr_26_30_percent" = "B25070_006",
    "hh_units"="B25001_001",
    "vacant"="B25002_003",
    "total_units"="B25002_001",
    "pov_100"="B07012_002",
    "pov_101_149"= "B07012_003",
    "owner_tax" = "B25103_001",
    "owner_val" = "B25097_001",
    "med_mort_payment"="B25088_002",
    "med_gross_rent_pct" = "B25071_001",
    'aff_am_pop' = "B02001_003"
  ),
  geography = "tract",
  cache_table = T,
  state = "PA",
  county = "Philadelphia",
  output = "wide",
  geometry = T
)

tract_aggs_hist = philly_hist[commercial ==F& case_dup == F & include == T,list(
  num_evict = .N
), by = .(xfileyear, GEOID.Tract )]

tract_aggs_cur = philly_cur[commercial ==F& case_dup == F & include == T,list(
  num_evict = .N
), by = .(xfileyear, GEOID.Tract )]

tracts_m= philly_census %>%
  merge(
    merge(tract_aggs_cur %>% filter(xfileyear == 2023),
          tract_aggs_hist %>% filter(xfileyear == 2019),
          by = c( "GEOID.Tract"),suffixes = c("_cur", "_hist"), all.x = T)
    , by.y = c("GEOID.Tract"), by.x = "GEOID", all.x = T) %>%
  mutate(
    num_evict_hist = replace_na(num_evict_hist, 0),
    num_evict_cur = replace_na(num_evict_cur, 0)
  )

tracts_m = tracts_m %>%
  mutate(
    evict_filing_rate_hist = 100 * num_evict_hist / renter_hhE,
    evict_filing_rate_cur = 100 * num_evict_cur / renter_hhE
    )

ggplot(tracts_m %>% filter(renter_hhE > 0), aes(fill = evict_filing_rate_hist)) +
  geom_sf(color = "white") +
  viridis::scale_fill_viridis(option = "H")

ggsave("figs/evict_filing_rate_hist.png", width = 10, height = 10)

# make cumulative distribution plot

tracts_m = tracts_m %>%
  arrange(num_evict_hist) %>%
  filter(renter_hhE > 0) %>%
  mutate(
    cum_per_evict_filings_hist = cumsum(num_evict_hist) / sum(num_evict_hist,na.rm = T),
    evict_ranking_hist = 100*row_number() / n()
  ) %>%
  arrange(num_evict_cur) %>%
  mutate(
    cum_per_evict_filings_cur = cumsum(num_evict_cur) / sum(num_evict_cur),
    evict_ranking_cur =  100*row_number() / n()
  )

ggplot(tracts_m, aes(x = evict_ranking_hist, y = cum_per_evict_filings_hist)) +
  #geom_line(aes(color = "2019")) +
  geom_point() +
  #geom_point(aes(x = evict_ranking_cur,y = cum_per_evict_filings_cur,color = "2023")) +
  scale_x_continuous(limits = c(0,100), breaks = seq(0,100,5)) +
  scale_y_continuous(limits = c(0,1), breaks = seq(0,1,.1)) +
  labs(
    x = "Tract ranking by evictions",
    y = "Cumulative percentage of evictions",
    title = "Cumulative distribution of evictions by Census Tract",
    subtitle = "Philadelphia, 2019"
  ) +
  theme_bw()

ggsave("figs/cumulative_evict_dist.png", width = 10, height = 10)


# same thing but by addresses
address_aggs_hist = philly_hist[commercial ==F& case_dup == F & include == T,list(
  num_evict = .N
), by = .(xfileyear, xdefendant_address_modal )]

address_aggs_cur = philly_cur[commercial ==F& case_dup == F & include == T,list(
  num_evict = .N
), by = .(xfileyear, xdefendant_address )]

address_m=  merge(address_aggs_cur %>% filter(xfileyear == 2023),
          address_aggs_hist %>% filter(xfileyear == 2019),
          suffixes = c("_cur", "_hist")
    , by.x = c("xdefendant_address"),
    by.y = "xdefendant_address_modal", all.x = T,all.y = T) %>%
  mutate(
    num_evict_hist = replace_na(num_evict_hist, 0),
    num_evict_cur = replace_na(num_evict_cur, 0)
  )

address_aggs_hist = address_aggs_hist %>%
  filter(xfileyear == 2019) %>%
  mutate(num_evict_hist = replace_na(num_evict, 0)) %>%
  arrange(num_evict_hist) %>%
  mutate(
    cum_per_evict_filings_hist = cumsum(num_evict_hist) / sum(num_evict_hist,na.rm = T),
    evict_ranking_hist = 100*row_number() / n()
  )

address_aggs_cur = address_aggs_cur %>%
  filter(xfileyear == 2023) %>%
  mutate(num_evict_cur = replace_na(num_evict, 0)) %>%
  arrange(num_evict_cur) %>%
  mutate(
    cum_per_evict_filings_cur = cumsum(num_evict) / sum(num_evict),
    evict_ranking_cur =  100*row_number() / n()
  )



ggplot(address_aggs_hist, aes(x = evict_ranking_hist, y = cum_per_evict_filings_hist)) +
  geom_point() +
  scale_x_continuous(limits = c(0,100), breaks = seq(0,100,5)) +
  scale_y_continuous(limits = c(0,1), breaks = seq(0,1,.1)) +
  labs(
    x = "Address ranking by evictions",
    y = "Cumulative percentage of evictions",
    title = "Cumulative distribution of evictions by Address",
    subtitle = "Philadelphia, 2019"
  ) +
  theme_bw()

ggsave("figs/cumulative_evict_dist_address_hist.png", width = 10, height = 10)

ggplot(address_aggs_cur, aes(x = evict_ranking_cur, y = cum_per_evict_filings_cur)) +
  geom_point() +
  geom_point(data= address_aggs_hist,
             aes(x = evict_ranking_hist,
                 y = cum_per_evict_filings_hist),
             color = "red") +
  scale_x_continuous(limits = c(0,100), breaks = seq(0,100,5)) +
  scale_y_continuous(limits = c(0,1), breaks = seq(0,1,.1)) +
  labs(
    x = "Address ranking by evictions",
    y = "Cumulative percentage of evictions",
    title = "Cumulative distribution of evictions by Address",
    subtitle = "Philadelphia, 2023"
  ) +
  theme_bw()

ggsave("figs/cumulative_evict_dist_address_cur.png", width = 10, height = 10)


# SAME but by plaintiff name
plaintiff_aggs_hist = philly_hist[commercial ==F& case_dup == F & include == T,list(
  num_evict = .N
), by = .(xfileyear, xplaintiff )]

plaintiff_aggs_cur = philly_cur[commercial ==F& case_dup == F & include == T,list(
  num_evict = .N
), by = .(xfileyear, xplaintiff )]


plaintiff_aggs_hist= plaintiff_aggs_hist %>%
  filter(xfileyear == 2019) %>%
  mutate(num_evict_hist = replace_na(num_evict, 0)) %>%
  arrange(num_evict_hist) %>%
  mutate(
    cum_per_evict_filings_hist = cumsum(num_evict_hist) / sum(num_evict_hist,na.rm = T),
    evict_ranking_hist = 100*row_number() / n()
  )

plaintiff_aggs_cur = plaintiff_aggs_cur %>%
  filter(xfileyear == 2023) %>%
  mutate(num_evict_cur = replace_na(num_evict, 0)) %>%
  arrange(num_evict_cur) %>%
  mutate(
    cum_per_evict_filings_cur = cumsum(num_evict) / sum(num_evict),
    evict_ranking_cur =  100*row_number() / n()
  )

ggplot(plaintiff_aggs_hist, aes(x = evict_ranking_hist, y = cum_per_evict_filings_hist)) +
  geom_point() +
  scale_x_continuous(limits = c(0,100), breaks = seq(0,100,5)) +
  scale_y_continuous(limits = c(0,1), breaks = seq(0,1,.1)) +
  labs(
    x = "Plaintiff ranking by evictions",
    y = "Cumulative percentage of evictions",
    title = "Cumulative distribution of evictions by Plaintiff",
    subtitle = "Philadelphia, 2019"
  ) +
  theme_bw()

ggsave("figs/cumulative_evict_dist_plaintiff_hist.png", width = 10, height = 10)

ggplot(plaintiff_aggs_cur, aes(x = evict_ranking_cur, y = cum_per_evict_filings_cur)) +
  geom_point() +
  geom_point(data= plaintiff_aggs_hist,
             aes(x = evict_ranking_hist,
                 y = cum_per_evict_filings_hist),
             color = "red") +
  scale_x_continuous(limits = c(0,100), breaks = seq(0,100,5)) +
  scale_y_continuous(limits = c(0,1), breaks = seq(0,1,.1)) +
  labs(
    x = "Plaintiff ranking by evictions",
    y = "Cumulative percentage of evictions",
    title = "Cumulative distribution of evictions by Plaintiff",
    subtitle = "Philadelphia, 2023"
  ) +
  theme_bw()

ggsave("figs/cumulative_evict_dist_plaintiff_cur.png", width = 10, height = 10)

philly_year_aggs = rbindlist(
  list(
    philly_hist[commercial ==F& case_dup == F & include == T,list(
      num_evict = .N
    ), by = .(xfileyear)],
    philly_cur[commercial ==F& case_dup == F & include == T,list(
      num_evict = .N
    ), by = .(xfileyear)]
  )
)

ggplot(philly_year_aggs %>% filter(xfileyear <= 2023), aes(x = xfileyear, y = num_evict)) +
  geom_bar(stat = "identity") +
  labs(
    x = "Year",
    y = "Number of evictions",
    title = "Evictions in Philadelphia by year"
  ) +
  scale_x_continuous(breaks = seq(2016,2024,1)) +
  theme_bw()

ggsave("figs/evict_by_year.png", width = 10, height = 10)

