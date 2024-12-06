library(data.table)
library(tidyverse)
library(ggplot2)
library(gt)

#philly_docket <- fread("~/Desktop/data/philly-evict/phila-lt-data/docket-entries.txt")
philly_summary <- fread("~/Desktop/data/philly-evict/phila-lt-data/summary-table.txt")


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
  writeLines("/Users/joefish/Documents/GitHub/philly-evictions/tables/philly-summary-rent.tex")


