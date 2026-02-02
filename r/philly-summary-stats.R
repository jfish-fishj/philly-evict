## ============================================================
## philly-summary-stats.R
## ============================================================
## Purpose: Generate summary statistics for Philadelphia evictions
##
## Inputs: evictions_summary_table, business_licenses, evictions_clean, parcels_clean
## Outputs: tables/philly-summary-rent.tex
## ============================================================

library(data.table)
library(tidyverse)
library(ggplot2)
library(gt)

# ---- Load config ----
source("r/config.R")
cfg <- read_config()

# ---- Load data via config ----
philly_summary <- fread(p_input(cfg, "evictions_summary_table"))
philly_summary[,xcasenum := id]

philly_lic = fread(p_input(cfg, "business_licenses"))
philly_evict = fread(p_product(cfg, "evictions_clean"))
philly_parcels = fread(p_product(cfg, "parcels_clean"))
#rgdal::ogrInfo(system.file("/Users/joefish/Desktop/data/philly-evict/opa_properties_public.gdb", package="sf"))

philly_rentals = philly_lic[licensetype == "Rental"]
philly_rentals[,start_year := as.numeric(substr(initialissuedate,1,4))]
philly_rentals[,end_year := as.numeric(substr(expirationdate,1,4))]
philly_rentals[,num_years := end_year - start_year]
philly_rentals[,id := .I]
philly_rentals[,pm.zip := coalesce(pm.zip, str_sub(zip,1,5)) %>%
                 str_pad(5, "left", pad = "0")]

philly_rentals = philly_rentals %>%
  mutate(
    geocode_y = lng,
    geocode_x = lat
  )


philly_rentals_long = uncount(philly_rentals[num_years > 0], num_years) %>%
  group_by(id) %>%
  mutate(
    year = start_year + row_number() - 1
  ) %>%
  ungroup() %>%
  as.data.table()

philly_rentals_long[,opa_account_num := str_pad(opa_account_num,9, "left",pad = "0")]
philly_rentals_long[,PID := opa_account_num]
philly_rentals_long = philly_rentals_long[year %in% 2016:2024 & n_sn_ss_c != ""]
philly_parcels[,PID := str_pad(parcel_number,9, "left","0")]
# now add in the

philly_evict[,pm.zip := coalesce(pm.zip,GEOID.Zip) %>%
               str_pad(5, "left", pad = "0")]


xwalk = fread(p_product(cfg, "evict_address_xwalk_case"))
xwalk[,PID := str_pad(as.character(PID),9, "left","0")]
#
philly_summary_m = merge(
  philly_summary,
  xwalk[!is.na(PID)], by = "xcasenum"
) %>%
  merge(
    philly_evict %>% select(-contains("pm.")), by = "xcasenum"
  )

philly_summary_m_parcel_agg = philly_summary_m[
  ongoing_rent <= 8000 &
    ongoing_rent >= 100 &
    !is.na(PID) &
    commercial.y==F
  & !str_detect(plaintiff,regex("Housing Authority", ignore_case = T))
  ,list(num_evict_parcel_year = .N,
        median_ongoing_rent = median(ongoing_rent,na.rm = T),
        mean_ongoing_rent = mean(ongoing_rent,na.rm = T),
        non_residential = first(non_residential),
        mean_total_rent = mean(total_rent,na.rm = T),
        median_total_rent = median(total_rent,na.rm = T),
        GEOID.Tract = first(GEOID.Tract)
        ),
  by = .(PID, xfileyear)]


philly_summary_m_parcel_agg[,num_evict_parcel_year_decile := ntile(num_evict_parcel_year,10), by = xfileyear]

philly_summary_m = merge(
  philly_summary_m_parcel_agg,
    philly_parcels %>% select(-contains("pm.")), by = "PID"
  )

philly_summary_m[xfileyear == 2019, list(median(median_ongoing_rent,na.rm = T),.N),
                 by =num_evict_parcel_year_decile ][order(num_evict_parcel_year_decile)]

philly_summary_m[xfileyear <= 2019, median(num_evict_parcel_year,na.rm = T),
                 by =num_evict_parcel_year_decile ][order(num_evict_parcel_year_decile)]

m1 <- fixest::feols(median_ongoing_rent ~ (num_evict_parcel_year) +poly(year_built,2) +
                poly(total_livable_area,2) +poly(number_of_bathrooms,2) +poly(number_of_bedrooms,2)
                 + fireplaces+garage_spaces

              |GEOID.Tract+central_air+exterior_condition+quality_grade +xfileyear ,
              data = philly_summary_m[xfileyear <= 2019 & num_evict_parcel_year_decile <= 10
                                      & !is.na(number_of_bathrooms) & number_of_bathrooms <= 4])

summary(m1)


philly_summary_m[,.(year_built, building_code, total_livable_area, central_air,
                    exterior_condition, fireplaces, garage_spaces, fuel,
                    number_of_bathrooms, number_of_bedrooms, date_exterior_condition)] %>%
  head()

## merge rental listings and evictions on xwalk
philly_rentals_long= philly_rentals_long %>% distinct(PID, year,.keep_all = T)
philly_rentals_evict_m = philly_rentals_long[PID != "" & !is.na(PID)] %>%
  merge(xwalk[!is.na(PID) & PID != "" & num_parcels_matched ==1 ], by = "PID",all.x = T) %>%
  mutate(xfileyear = year,n_sn_ss_c= n_sn_ss_c.y) %>%
  merge(philly_evict_address_agg[!is.na(n_sn_ss_c),list(num_evict = first(num_evict)), by = .(n_sn_ss_c, xfileyear)],
        by = c("n_sn_ss_c","xfileyear"),all.x = T)

philly_rentals_evict_m[,num_evict := fifelse(is.na(num_evict), 0, num_evict)]

# parcels agg
parcels_agg = philly_rentals_evict_m[rentalcategory!= "Hotel",list(
  num_evict = sum(num_evict),
  num_units = first(numberofunits)
  #num_addys = uniqueN(n_sn_ss_c)
), by = .(xfileyear, PID)]

# make sure i get about the same number of evictions
philly_evict_address_agg[,sum(num_evict), by =xfileyear][order(xfileyear)]
philly_rentals_evict_m[,sum(num_evict), by =xfileyear][order(xfileyear)]
parcels_agg[,sum(num_evict), by =xfileyear][order(xfileyear)]
parcels_agg[,sum(num_units), by =xfileyear][order(xfileyear)]

parcels_agg = parcels_agg %>%
  mutate(
    evict_filing_rate = num_evict / num_units
  )%>%
  merge(philly_parcels[,.(PID, category_code_description,owner_1)], by = "PID", all.x = T) %>%
  filter(!category_code_description %in% c("HOTEL","GARAGE - COMMERCIAL","OFFICES",
                                           "VACANT LAND","COMMERCIAL") ) %>%
  # remove student housing
  filter(!str_detect(owner_1, "PHILADELPHIA UNIVERSITY|TEMPLE UNI|DREXEL|UNIVERSITY|UNIV OF")) %>%
  group_by(xfileyear) %>%
  arrange(num_evict) %>%
  mutate(
    cum_per_evict_filings = cumsum(num_evict) / sum(num_evict,na.rm = T),
    cum_per_units = cumsum(num_units) / sum(num_units,na.rm = T),
    evict_ranking = 100*row_number() / n()
  ) %>%
  ungroup() %>%
  as.data.table()



philly_summary_plaintiff = philly_summary[ ,list(num_evict_year = .N), by = .(plaintiff, year)]

philly_summary_plaintiff[,num_evict_year_decile := ntile(num_evict_year,10), by = year]
philly_summary_plaintiff[,num_evict_year_percentile := ntile(num_evict_year,100), by = year]
philly_summary_plaintiff[,num_evict_year_rank := frankv(x=num_evict_year, order = c(-1)), by = year]
philly_summary_plaintiff[,high_evict := num_evict_year_rank <= 100]

philly_summary = merge(
  philly_summary,
  philly_summary_plaintiff[,.(plaintiff, year, num_evict_year, num_evict_year_decile,
                              num_evict_year_percentile, num_evict_year_rank, high_evict)],
  by = c("plaintiff", "year"),
  all.x = T
)


# summary stats on philly rent prices

philly_summary_rent <- philly_summary %>%
  filter(commercial == "f"& year %in% 2010:2019 & non_residential=="f" & lead_subsidized=="f" & ongoing_rent <= 10000 &ongoing_rent >= 100) %>%
  distinct(year, month, d_filing, defendant_address, defendant, ongoing_rent,.keep_all = T) %>%
  group_by(year) %>%
  summarise(
    mean_rent = mean(ongoing_rent,na.rm = T),
    median_rent = median(ongoing_rent,na.rm = T),
    sd_rent = sd(ongoing_rent,na.rm = T),
    num_evict = n()
  )

philly_summary_rent_high_evict <- philly_summary %>%
  filter(commercial == "f"& year %in% 2010:2019& non_residential=="f" & lead_subsidized=="f"  & ongoing_rent <= 10000 &ongoing_rent >= 100) %>%
  distinct(year, month, d_filing, defendant_address, defendant, ongoing_rent,.keep_all = T) %>%
  group_by(year, high_evict) %>%
  summarise(
    mean_rent = mean(ongoing_rent,na.rm = T),
    median_rent = median(ongoing_rent,na.rm = T),
    sd_rent = sd(ongoing_rent,na.rm = T),
    num_evict = n()
  ) %>%
  ungroup() %>%
  pivot_wider(names_from = high_evict, values_from = c(mean_rent, median_rent, sd_rent,num_evict))

philly_summary_rent_high_evict_table = philly_summary_rent_high_evict %>%
  select(-matches("mean|sd_")) %>%
  #  rename(
  #   `Median Rent (non-Top Evictor)` = median_rent_FALSE,
  #   `Median Rent (Top Evictor)` = median_rent_TRUE,
  #   `Num Evict (non-Top Evictor)` = num_evict_FALSE,
  #   `Num Evict (Top Evictor)` = num_evict_TRUE
  # ) %>%
  gt(
    rowname_col = "year"
  ) %>%
  tab_header(
    title = "Summary Statistics on Philadelphia Rent Prices",
    subtitle = "2010-2019"
  ) %>%
  cols_label(
    year = "Year",
    median_rent_FALSE = "non-Top Evictor",
    median_rent_TRUE = "Top Evictor",
    num_evict_FALSE = "non-Top Evictor",
    num_evict_TRUE = "Top Evictor"
  ) %>%
  gt::tab_spanner(
    label = c("Rent"),
    columns = c(median_rent_FALSE, median_rent_TRUE)
  ) %>%
  tab_spanner(
    label = c("Number of Evictions"),
    columns = c(num_evict_FALSE, num_evict_TRUE)
  ) %>%
  fmt_number(
    columns = c(median_rent_FALSE, median_rent_TRUE),
    decimals = 0
  )

philly_summary_rent_high_evict_table %>%
  as_latex() %>%
  writeLines(p_out(cfg, "tables", "philly-summary-rent.tex"))


