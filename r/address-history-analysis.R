library(data.table)
library(tidyverse)
library(sf)
library(tidycensus)
library(fixest)
philly_infousa_dt = fread("~/Desktop/data/philly-evict/infousa_address_cleaned.csv")
philly_infousa_dt[,obs_id := .I]
philly_parcels = fread("~/Desktop/data/philly-evict/processed/parcel_building_2024.csv")
info_usa_xwalk = fread("/Users/joefish/Desktop/data/philly-evict/philly_infousa_dt_address_agg_xwalk.csv")
philly_rent_df = fread("~/Desktop/data/philly-evict/processed/bldg_panel.csv")
philly_rentals = fread("~/Desktop/data/philly-evict/processed/license_long_min.csv")
philly_occ = fread("~/Desktop/data/philly-evict/processed/infousa_parcel_occupancy_vars.csv")
# Merge infousa with parcel data
parcel_cols = intersect(philly_rent_df %>% colnames(), philly_parcels %>% colnames())

# evict_df
philly_evict_df = philly_rent_df[,list(
  filing_rate = mean(filing_rate),
  total_filings = sum(num_filings)  ), by = PID]

# rentals
rental_PIDs = philly_rentals[,unique(PID)] %>%
  append(philly_rent_df[,unique(PID)]) %>%
  append(info_usa_xwalk[n_sn_ss_c %in% philly_infousa_dt[owner_renter_status %in% 0:3 , n_sn_ss_c] , unique(PID)]) %>%
  unique()

philly_infousa_dt_m = merge(
  philly_infousa_dt,
  info_usa_xwalk[num_parcels_matched ==1 & !is.na(PID)],
  by = "n_sn_ss_c"
)

philly_infousa_dt_m = philly_infousa_dt_m %>%
  merge(philly_occ, c("PID", "year"), all.x = T)

# merge on parcel chars
philly_parcels[,PID := as.numeric(PID)]
philly_infousa_dt_m = merge(philly_infousa_dt_m %>% select(-matches("parcels")),
                            philly_parcels,
                            suffixes = c("", "_parcels")
                            , by = "PID",
                            all.x = T)

# merge on rental prices
philly_infousa_dt_m = philly_infousa_dt_m %>%
  merge(philly_rent_df %>%
          select(PID, year, med_price, log_med_rent, med_price_unadj),
        by = c("PID", "year"),
        all.x = T)

philly_infousa_dt_m[,total_filings := replace_na(total_filings, 0)]
philly_infousa_dt_m[,filing_rate := replace_na(filing_rate, 0)]

philly_infousa_dt_m[,rental := PID %in% rental_PIDs]
philly_infousa_dt_m[,.N, by= rental]
philly_infousa_dt_m[,list(count = .N, sum_rental = sum(rental==T)),
                    by = owner_renter_status][, per := round(sum_rental/count,2)][order(owner_renter_status)]

philly_infousa_dt_m[order(year),prev_address := data.table::shift(n_sn_ss_c), by = familyid ]
philly_infousa_dt_m[order(year),prev_evict := data.table::shift(filing_rate), by = familyid ]
philly_infousa_dt_m[order(year),prev_rental := data.table::shift(rental), by = familyid ]
philly_infousa_dt_m[order(year),prev_num_units_imp := data.table::shift(num_units_imp), by = familyid ]
philly_infousa_dt_m[order(year),prev_ge_census_tract := data.table::shift(ge_census_tract), by = familyid ]
philly_infousa_dt_m[order(year),prev_ct_id_10 := data.table::shift(CT_ID_10), by = familyid ]
philly_infousa_dt_m[order(year),prev_pm.zip := data.table::shift(pm.zip), by = familyid ]
philly_infousa_dt_m[order(year),prev_log_med_rent := data.table::shift(log_med_rent), by = familyid ]

philly_infousa_dt_m[,move := n_sn_ss_c != prev_address]
philly_infousa_dt_m_movers = philly_infousa_dt_m[move == T
                                                 & !is.na(prev_address)
                                                 & rental ==T
                                                 &!is.na(familyid)
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
philly_infousa_dt_m_movers[,imputed_rent := mean(log_med_rent, na.rm=TRUE), by = pid]
philly_infousa_dt_m_movers[,imputed_prev_rent := mean(prev_log_med_rent, na.rm=TRUE), by = pid]

m1 <- feols(filing_rate ~ prev_evict ,#|num_units_bins + prev_num_units_bins+ct_id_10 + prev_ct_id_10,
      cluster = ~pid,
      data = philly_infousa_dt_m_movers[(filing_rate  <= 1 & prev_evict <= 1) ])

m2 <- feols(filing_rate ~ prev_evict|num_units_bins + prev_num_units_bins+ct_id_10 + prev_ct_id_10,
      cluster = ~pid,
      data = philly_infousa_dt_m_movers[filing_rate <= 1 & prev_evict <= 1])

m3 <- feols(filing_rate ~ prev_evict +imputed_rent + imputed_prev_rent|num_units_bins   + prev_num_units_bins+ct_id_10 + prev_ct_id_10,
      cluster = ~pid,
      data = philly_infousa_dt_m_movers[filing_rate <= 0.5 & prev_evict <= 0.5   ])

m4 <- feols(filing_rate ~ prev_evict + log_med_rent + prev_log_med_rent|num_units_bins   + prev_num_units_bins+ct_id_10 + prev_ct_id_10,
      cluster = ~pid,
      data = philly_infousa_dt_m_movers[filing_rate <= 1 & prev_evict <=1  ])

philly_infousa_dt_m_movers[,high_filing := filing_rate > 0.1]
philly_infousa_dt_m_movers[,high_filing_prev := prev_evict > 0.1]

feols(high_filing ~ high_filing_prev|num_units_bins + prev_num_units_bins+ct_id_10 + prev_ct_id_10,
      cluster = ~pid,
      data = philly_infousa_dt_m_movers[filing_rate <= 0.5 & prev_evict <= 0.5  ])

m5 <- feols(high_filing ~ high_filing_prev + log_med_rent + prev_log_med_rent|num_units_bins + prev_num_units_bins+ct_id_10 + prev_ct_id_10,
      cluster = ~pid,
      data = philly_infousa_dt_m_movers[filing_rate <= 0.5 & prev_evict <= 0.5 & num_units_imp > 1 & prev_num_units_imp > 1 ])

feols(log_med_rent ~ prev_evict+ prev_log_med_rent +filing_rate|num_units_bins + prev_num_units_bins+ct_id_10 + prev_ct_id_10,
      cluster = ~pid,
      data = philly_infousa_dt_m_movers[filing_rate <= 0.5 & prev_evict <= 0.5  ])

# set default style
def_style = style.df(depvar.title = "", fixef.title = "",
                     fixef.suffix = " fixed effect", yesNo = c("Yes","No"))

fixest::setFixest_etable(
  digits = 4, fitstat = c("n")
)

setFixest_dict(
  num_units_bins = "Units (current)",
  prev_num_units_bins = "Units (previous)",
  ct_id_10 = "Census Tract (current)",
  prev_ct_id_10 = "Census Tract (previous)",
  prev_evict = "Previous Eviction Filing Rate",
  imputed_rent = "Imputed Rent (current)",
  imputed_prev_rent = "Imputed Rent (previous)",
  log_med_rent = "Rent (current)",
  prev_log_med_rent = "Rent (previous)"
)

etable(list(m1, m2,  m4),
       title = "Effect of Previous Eviction Filing Rate on Current Filing Rate",
       #style = def_style,
       label = "tab:evict_persist",
       drop = "Constant",
       tex = T
) %>% writeLines("/Users/joefish/Documents/GitHub/philly-evictions/tables/evict_persist.tex")

# get eviction rate of prev move
philly_infousa_dt_m_movers[prev_num_units_imp >= 1 & num_units_imp >= 1
                           & !is.na(log_med_rent) & !is.na(prev_log_med_rent)
                           & filing_rate <= 0.5 & prev_evict <= 0.5,
                           cor(prev_evict, filing_rate, use = "complete.obs")]

philly_infousa_dt_m_movers[prev_num_units_imp >= 1 & num_units_imp >= 1 & filing_rate <= 0.5 & prev_evict <= 0.5,
                           cor(log_med_rent, prev_log_med_rent, use = "complete.obs")]

# bin scatter that shit
philly_infousa_dt_m_movers[,`:=`(
  # round evict to 0.05 bins
  prev_evict_bin = round(prev_evict / 0.05) * 0.05,
  evict_bin = round(filing_rate / 0.05) * 0.05
)]

# replace > 0.25 with 0.25
philly_infousa_dt_m_movers[prev_evict_bin > 0.25, prev_evict_bin := 0.25]
philly_infousa_dt_m_movers[evict_bin > 0.25, evict_bin := 0.25]

# do one of those flow plots of counts
philly_infousa_dt_m_movers_flows = philly_infousa_dt_m_movers[
  !is.na(prev_evict_bin) & !is.na(evict_bin) &
    num_units_imp > 1 & prev_num_units_imp > 1 &
    prev_evict <= 0.5 & filing_rate <= 0.5 ,.N, by = .(
  prev_evict_bin,
  evict_bin
)][order(prev_evict_bin, evict_bin)]

# make percents
philly_infousa_dt_m_movers_flows[, total_from_bin := sum(N)]
philly_infousa_dt_m_movers_flows[, percent := round(N / total_from_bin,3)]

# make relative eprcent
philly_infousa_dt_m_movers_flows[, total_to_bin := sum(N), by = evict_bin]
philly_infousa_dt_m_movers_flows[, percent_to := round(N / total_to_bin,3)]

# make bins into factors
philly_infousa_dt_m_movers_flows[,`:=`(
  prev_evict_bin_factor = as.factor(prev_evict_bin),
  evict_bin_factor = as.factor(evict_bin)
)]

library(ggalluvial)
ggplot(philly_infousa_dt_m_movers_flows[evict_bin == 0.0],
       aes(axis1 = prev_evict_bin_factor, axis2 = evict_bin_factor, y = percent_to)) +
  scale_x_discrete(limits = c("Input", "Output"), expand = c(.1, .05)) +
  geom_alluvium(aes(fill = prev_evict_bin_factor), width = 1/12, alpha = 0.9) +
  geom_stratum(width = 1/12, fill = "grey90", color = "grey40") +
  geom_text(stat = "stratum", aes(label = after_stat(stratum)), size = 3.5) +
  labs(title = "Flows from Inputs to Outputs",
       y = "Weight", x = NULL, fill = "Input") +
  scale_fill_viridis_d(option = "C") +  # colorblind-friendly
  theme_minimal(base_size = 12) +
  theme(panel.grid = element_blank())


# of high evicting units how many move into non-high evicting
philly_infousa_dt_m_movers[,high_filing := filing_rate > 0.1]
philly_infousa_dt_m_movers[,high_filing_prev := prev_evict > 0.1]

philly_infousa_dt_m_movers[,.N, by = high_filing]
philly_infousa_dt_m_movers[,.N, by = high_filing_prev]

# bin rentals
philly_infousa_dt_m_movers[,imp_rent_decile := ntile(imputed_rent, 10), by = year]
philly_infousa_dt_m_movers[,imp_prev_rent_decile := ntile(imputed_prev_rent, 10), by = year]

philly_infousa_dt_m_movers[,low_rent :=imp_rent_decile <= 2]
philly_infousa_dt_m_movers[,low_prev_rent :=imp_prev_rent_decile <= 2]

philly_infousa_dt_m_movers[prev_num_units_imp > 1 & num_units_imp >1,list(
  count = .N,
  from_high_to_low = sum(high_filing == F & high_filing_prev == T),
  from_low_to_high = sum(high_filing == T & high_filing_prev == F),
  stay_high = sum(high_filing == T & high_filing_prev == T),
  stay_low = sum(high_filing == F & high_filing_prev == F)
)] %>% mutate(across(where(is.numeric), ~ .x / count))

# repeat for rents
philly_infousa_dt_m_movers[prev_num_units_imp > 1 & num_units_imp >1,list(
  count = sum(!is.na(low_rent) & !is.na(low_prev_rent)),
  from_high_to_low = sum(low_rent == F & low_prev_rent == T,na.rm=T),
  from_low_to_high = sum(low_rent == T & low_prev_rent == F,na.rm=T),
  stay_high = sum(low_rent == T & low_prev_rent == T,na.rm=T),
  stay_low = sum(low_rent == F & low_prev_rent == F,na.rm=T)
)] %>% mutate(across(where(is.numeric), ~ .x / count))

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


ggplot(philly_infousa_dt_m_movers[filing_rate <= 0.5 &
                                    prev_evict <= 0.5 &
                                    prev_num_units_imp > 1 &
                                    num_units_imp > 1], aes(x = prev_evict, y = filing_rate))+
  geom_point(alpha = 0.1) +
  geom_smooth()+
  geom_abline(slope = 1, intercept = 0, color = "red")


