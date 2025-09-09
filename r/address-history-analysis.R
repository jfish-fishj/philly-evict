library(data.table)
library(tidyverse)
library(sf)
library(tidycensus)

philly_infousa_dt = fread("~/Desktop/data/philly-evict/infousa_address_cleaned.csv")
philly_infousa_dt[,obs_id := .I]
philly_parcels = fread("~/Desktop/data/philly-evict/processed/parcel_building_2024.csv")
info_usa_xwalk = fread("/Users/joefish/Desktop/data/philly-evict/philly_infousa_dt_address_agg_xwalk.csv")
philly_rent_df = fread("~/Desktop/data/philly-evict/processed/bldg_panel.csv")
philly_rentals = fread("~/Desktop/data/philly-evict/processed/license_long_min.csv")

# Merge infousa with parcel data
parcel_cols = intersect(philly_rent_df %>% colnames(), philly_parcels %>% colnames())

# evict_df
philly_evict_df = philly_rent_df[,list(
  filing_rate = mean(filing_rate),
  total_filings = sum(num_filings)  ), by = PID]

# rentals
rental_PIDs = philly_rentals[,unique(PID)] %>%
  append(philly_rent_df[,unique(PID)]) %>%
  unique()
philly_infousa_dt_m = merge(
  philly_infousa_dt,
  info_usa_xwalk[num_parcels_matched ==1 & !is.na(PID)],
  by = "n_sn_ss_c"
)

philly_infousa_dt_m = philly_infousa_dt_m %>%
  merge(philly_evict_df, by = "PID", all.x = T)

# merge on parcel chars
philly_parcels[,PID := as.numeric(PID)]
philly_infousa_dt_m = merge(philly_infousa_dt_m %>% select(-matches("parcels")),
                            philly_parcels,
                            suffixes = c("", "_parcels")
                            , by = "PID",
                            all.x = T)

philly_infousa_dt_m[,total_filings := replace_na(total_filings, 0)]
philly_infousa_dt_m[,filing_rate := replace_na(filing_rate, 0)]

philly_infousa_dt_m[,rental := PID %in% rental_PIDs]
philly_infousa_dt_m[,.N, by= rental]
philly_infousa_dt_m[,list(count = .N, sum_rental = sum(rental==T)),
                    by = OWNER_RENTER_STATUS][, per := round(sum_rental/count,2)][order(OWNER_RENTER_STATUS)]

philly_infousa_dt_m[order(year),prev_address := data.table::shift(n_sn_ss_c), by = FAMILYID ]
philly_infousa_dt_m[order(year),prev_evict := data.table::shift(filing_rate), by = FAMILYID ]
philly_infousa_dt_m[order(year),prev_rental := data.table::shift(rental), by = FAMILYID ]
philly_infousa_dt_m[order(year),prev_num_units_imp := data.table::shift(num_units_imp), by = FAMILYID ]
philly_infousa_dt_m[order(year),prev_GE_CENSUS_TRACT := data.table::shift(GE_CENSUS_TRACT), by = FAMILYID ]
philly_infousa_dt_m[order(year),prev_ct_id_10 := data.table::shift(CT_ID_10), by = FAMILYID ]
philly_infousa_dt_m[order(year),prev_pm.zip := data.table::shift(pm.zip), by = FAMILYID ]

philly_infousa_dt_m[,move := n_sn_ss_c != prev_address]
philly_infousa_dt_m_movers = philly_infousa_dt_m[move == T
                                                 & !is.na(prev_address)
                                                 & rental ==T
                                                 & prev_rental==T]
philly_infousa_dt_m_movers[, num_units_bins := fifelse(num_units_imp == 1, "1",
                                                       fifelse(num_units_imp <= 5, "2-5",
                                                               fifelse(num_units_imp <= 50, "6-50",
                                                                       fifelse(num_units_imp <= 100, "51-100",
                                                                               fifelse(num_units_imp > 100, "101+", NA_character_)))))]
philly_infousa_dt_m_movers[,prev_num_units_bins := fifelse(prev_num_units_imp == 1, "1",
                                                       fifelse(prev_num_units_imp <= 5, "2-5",
                                                               fifelse(prev_num_units_imp <= 50, "6-50",
                                                                       fifelse(prev_num_units_imp <= 100, "51-100",
                                                                               fifelse(prev_num_units_imp > 100, "101+", NA_character_)))))]
philly_infousa_dt_m_movers = philly_infousa_dt_m_movers %>% janitor::clean_names()
feols(filing_rate ~ prev_evict|num_units_bins + prev_num_units_bins+ct_id_10 + prev_ct_id_10,
      cluster = ~pid,
      data = philly_infousa_dt_m_movers[filing_rate <= 1 & prev_evict <= 1])

# get eviction rate of prev move
philly_infousa_dt_m_movers[prev_num_units_imp > 1 & num_units_imp >= 1 & filing_rate <= 1 & prev_evict <= 1,
                           cor(prev_evict, filing_rate, use = "complete.obs")]

# bin scatter that shit
philly_infousa_dt_m_movers[,`:=`(
  # round evict to 0.05 bins
  prev_evict_bin = round(prev_evict / 0.05) * 0.05
)]

# of high evicting units how many move into non-high evicting
philly_infousa_dt_m_movers[,high_filing := filing_rate > 0.1]
philly_infousa_dt_m_movers[,high_filing_prev := prev_evict > 0.1]

philly_infousa_dt_m_movers[,.N, by = high_filing]

philly_infousa_dt_m_movers[prev_num_units_imp > 1 & num_units_imp > 1,list(
  count = .N,
  from_high_to_low = sum(high_filing == F & high_filing_prev == T),
  from_low_to_high = sum(high_filing == T & high_filing_prev == F),
  stay_high = sum(high_filing == T & high_filing_prev == T),
  stay_low = sum(high_filing == F & high_filing_prev == F)
)]

# try to get a line that would be what people would move to if they moved randomly
philly_infousa_dt_m_movers[filing_rate <= 1, mean(filing_rate)]


bin_scatter = philly_infousa_dt_m_movers[filing_rate <= 1 &
                                         prev_num_units_imp > 1 & num_units_imp > 1
                                         & prev_evict_bin <= 1,list(
  mean_evict = mean(filing_rate),
  .N
), by = prev_evict_bin]


# sim some data
probs = philly_infousa_dt_m_movers[filing_rate <= 1 & prev_evict <= 1 &prev_num_units_imp > 1 & num_units_imp > 1, list(
 .N
), by = high_filing][,prob := N / sum(N)] %>% pull(prob)

probs_evict_bins = philly_infousa_dt_m_movers[filing_rate <= 1 & prev_evict <= 1 &prev_num_units_imp > 1 & num_units_imp > 1, list(
  .N
), by = prev_evict_bin][,prob := N / sum(N)]

sim_df = data.table(
  high_filing = sample(c(F,T), size = 10000, replace = T, prob = probs),
  high_filing_prev = sample(c(F,T), size = 10000, replace = T, prob = probs)
)

sim_df[,list(
  count = .N,
  from_high_to_low = sum(high_filing == F & high_filing_prev == T),
  from_low_to_high = sum(high_filing == T & high_filing_prev == F),
  stay_high = sum(high_filing == T & high_filing_prev == T),
  stay_low = sum(high_filing == F & high_filing_prev == F)
)] %>% mutate(across(where(is.numeric), ~ .x / count))

philly_infousa_dt_m_movers[prev_num_units_imp > 1 & num_units_imp > 1,list(
  count = .N,
  from_high_to_low = sum(high_filing == F & high_filing_prev == T),
  from_low_to_high = sum(high_filing == T & high_filing_prev == F),
  stay_high = sum(high_filing == T & high_filing_prev == T),
  stay_low = sum(high_filing == F & high_filing_prev == F)
)] %>% mutate(across(where(is.numeric), ~ .x / count))

sim_df_evict = data.table(
  filing_rate = sample(probs_evict_bins$prev_evict_bin, size = 100000, replace = T, prob = probs_evict_bins$prob),
  prev_evict = sample(probs_evict_bins$prev_evict_bin, size = 100000, replace = T, prob = probs_evict_bins$prob)
)

feols(filing_rate ~ prev_evict, data = sim_df_evict)

ggplot(sim_df_evict, aes(x = prev_evict, y = filing_rate))+
  geom_point(alpha = 0.1) +
  geom_smooth()+
  geom_abline(slope = 1, intercept = 0, color = "red")


ggplot(bin_scatter[mean_evict <= 1 & prev_evict_bin <= 1], aes(x = prev_evict_bin, y = mean_evict))+
  geom_point() +
  geom_smooth()+
  geom_smooth(method = "lm", color = "black")+
  geom_abline(slope = 1, intercept = 0, color = "red")


philly_infousa_dt_m_movers = janitor::clean_names(philly_infousa_dt_m_movers)
ggplot(philly_infousa_dt_m_movers[filing_rate <= 1 &
                                    prev_evict <= 1 &
                                    prev_num_units_imp > 1 &
                                    num_units_imp > 1], aes(x = prev_evict, y = filing_rate))+
  geom_point(alpha = 0.1) +
  geom_smooth()+
  geom_abline(slope = 1, intercept = 0, color = "red")


