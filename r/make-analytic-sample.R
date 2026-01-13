## ============================================================
## make-analytic-sample-from-ever-rentals.R
## ============================================================
## Assumes:
##  - ever_rentals_panel.csv          (from ever-rentals script)
##  - parcel_occupancy_panel_with_ever_rentals.csv (from occupancy script)
##  - parcel_events_panel_pid_year.csv (violations + permits + complaints, PID×year)
##  - assessments.csv                 (raw assessments: taxable_land, taxable_building, parcel_number, year, ...)
##
## Produces:
##  - bldg_panel_blp.csv   : full panel with shares, HHI, BLP instruments
##  - analytic_sample.csv  : filtered estimation sample
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(tidyverse)
})

normalize_pid <- function(x) {
  stringr::str_pad(as.character(x), width = 9, side = "left", pad = "0")
}
fco <- function(x, val = 0) fifelse(is.na(x), val, x)

## ------------------------------------------------------------
## 0) PATHS (EDIT THESE)
## ------------------------------------------------------------
ROOT      <- "~/Desktop/data/philly-evict"
PROC      <- file.path(ROOT, "processed")

path_ever_panel    <- file.path(PROC, "ever_rentals_panel.csv")
path_occ_panel     <- file.path(PROC, "parcel_occupancy_panel_with_ever_rentals.csv")
path_events_panel  <- file.path(PROC, "parcel_building.csv")

## Raw assessments file (as in original: ass <- fread(".../assessments.csv"))
path_assess_csv    <- file.path(ROOT, "assessments.csv")

out_bldg_panel     <- file.path(PROC, "bldg_panel_blp.csv")
out_analytic       <- file.path(PROC, "analytic_sample.csv")

## ------------------------------------------------------------
## 1) LOAD DATA
## ------------------------------------------------------------
ever_panel    <- fread(path_ever_panel)
occ_panel     <- fread(path_occ_panel)
event_cols <- (fread(path_events_panel, nrows = 1) |>  colnames())[ (1:35)]
events_panel  <- fread(path_events_panel, select = event_cols)
ass           <- fread(path_assess_csv)

setDT(ever_panel)
setDT(occ_panel)
setDT(events_panel)
setDT(ass)

## Normalize PIDs
ever_panel[,   PID := normalize_pid(PID)]
occ_panel[,    PID := normalize_pid(PID)]
events_panel[, PID := normalize_pid(PID)]

## ------------------------------------------------------------
## 2) BUILD BASE PANEL (PID × YEAR) FROM OCC + EVER + EVENTS
## ------------------------------------------------------------
bldg_panel <- copy(occ_panel)

# have to clean up that pm.zip.x; pm.zip.y are in
ever_panel[,pm.zip := coalesce(pm.zip.x |> as.numeric(),
                               pm.zip.y|> as.numeric(),
                               zip_code|> as.numeric() )]

## ever_rentals_panel: rents, rental flags, filings, parcel chars
bldg_panel <- merge(
  bldg_panel,
  ever_panel,
  by    = c("PID", "year"),
  all.x = TRUE,
  suffixes = c("", "_ever")
)

## Single merged events file: violations + permits + complaints (PID×year)
bldg_panel <- merge(
  bldg_panel,
  events_panel,
  by    = c("PID", "year"),
  all.x = TRUE
)

## ------------------------------------------------------------
## 3) ASSESSMENTS: BUILD VARIABLES ON THE FLY (from ass <- fread(...))
## ------------------------------------------------------------

## Normalize PID & year in assessments
ass[, PID  := normalize_pid(parcel_number)]
ass[, year := as.integer(year)]

## Core taxable values
ass[, taxable_value := as.numeric(taxable_land + taxable_building)]

## Yearly changes
ass[order(year),
    change_taxable_value := taxable_value - data.table::shift(taxable_value),
    by = PID]

ass[order(year),
    pct_change_taxable_value := change_taxable_value / data.table::shift(taxable_value),
    by = PID]

## Range of changes per parcel
ass[, min_change_taxable_value := min(change_taxable_value, na.rm = TRUE), by = PID]
ass[, max_change_taxable_value := max(change_taxable_value, na.rm = TRUE), by = PID]
ass[, range_change_taxable_value := max_change_taxable_value - min_change_taxable_value]

## Clean pct_change & compute pct-change range
ass[!is.finite(pct_change_taxable_value), pct_change_taxable_value := NA_real_]
ass[, min_pct_change_taxable_value := min(pct_change_taxable_value, na.rm = TRUE), by = PID]
ass[, max_pct_change_taxable_value := max(pct_change_taxable_value, na.rm = TRUE), by = PID]
ass[, range_pct_change_taxable_value := max_pct_change_taxable_value - min_pct_change_taxable_value]

## Log taxable values and changes
ass[, log_taxable_value := log(taxable_value)]
ass[, log_taxable_value := fifelse(is.finite(log_taxable_value), log_taxable_value, NA_real_)]

ass[order(PID, year),
    change_log_taxable_value := log_taxable_value - data.table::shift(log_taxable_value),
    by = PID]
ass[, change_log_taxable_value := fifelse(is.finite(change_log_taxable_value), change_log_taxable_value, NA_real_)]

## Lags
ass[order(year),
    taxable_value_lag1 := data.table::shift(taxable_value),
    by = PID]
ass[order(year),
    taxable_value_lag2 := data.table::shift(taxable_value, 2),
    by = PID]
ass[order(year),
    change_taxable_value_lag1 := data.table::shift(change_taxable_value),
    by = PID]
ass[order(year),
    pct_change_taxable_value_lag1 := data.table::shift(pct_change_taxable_value),
    by = PID]

## Max abs log-change used to flag "crazy" parcels
ass[, max_abs_change_log_taxable_value := max(abs(change_log_taxable_value), na.rm = TRUE), by = PID]

## Set all *taxable* variables to NA for parcels with huge swings
tax_cols <- grep("taxable", names(ass), value = TRUE)
ass[max_abs_change_log_taxable_value >= 2,
    (tax_cols) := lapply(.SD, function(x) NA_real_),
    .SDcols = tax_cols]

## Drop range and min/max helper columns before merging
drop_cols <- c(
  "parcel_number",
  grep("range", names(ass), value = TRUE),
  grep("min_", names(ass),  value = TRUE),
  grep("max_", names(ass),  value = TRUE)
)
drop_cols <- intersect(drop_cols, names(ass))
if (length(drop_cols) > 0L) ass[, (drop_cols) := NULL]

## Merge assessments into bldg_panel (PID×year)
bldg_panel <- merge(
  bldg_panel,
  ass,
  by    = c("PID", "year"),
  all.x = TRUE
)

## Per-unit assessment vars (using canonical units from occupancy)
if (!("num_units_imp_final" %in% names(bldg_panel))) {
  stop("Expected 'num_units_imp_final' in occupancy panel.")
}

bldg_panel[
  ,
  change_taxable_value_per_unit := fifelse(
    !is.na(change_taxable_value) & num_units_imp_final > 0,
    change_taxable_value / num_units_imp_final,
    NA_real_
  )
]
bldg_panel[
  ,
  log_taxable_value_per_unit := fifelse(
    !is.na(taxable_value) & taxable_value > 0 & num_units_imp_final > 0,
    log(taxable_value / num_units_imp_final),
    NA_real_
  )
]

## ------------------------------------------------------------
## 4) BASIC CLEANUP AND CORE VARIABLES
## ------------------------------------------------------------

## Units & occupancy (from occupancy script)
bldg_panel[
  ,
  occupied_units := fifelse(
    !is.na(occupancy_rate),
    num_units_imp_final * occupancy_rate,
    num_units_imp_final
  )
]

## Rent variable (Altos)
bldg_panel[,med_rent := coalesce(
  med_rent_altos,
  med_eviction_rent,
)]
bldg_panel[
  ,
  log_med_rent := fifelse(
    !is.na(med_rent) & med_rent > 0,
    log(med_rent),
    NA_real_
  )
]

## ------------------------------------------------------------
## 5) MARKET & OWNER IDS
## ------------------------------------------------------------

## Market = zip-year (pm.zip_parcels preferred)
market_zip_col <- if ("pm.zip_parcels" %in% names(bldg_panel)) {
  "pm.zip_parcels"
} else if ("pm.zip" %in% names(bldg_panel)) {
  "pm.zip"
} else if ("zip_parcels" %in% names(bldg_panel)) {
  "zip_parcels"
} else {
  NA_character_
}
if (is.na(market_zip_col)) {
  stop("No zip column found (pm.zip_parcels / pm.zip / zip_parcels). Please adjust the market definition.")
}

bldg_panel[
  ,
  market_zip := str_pad(as.character(get(market_zip_col)), width = 5, side = "left", pad = "0")
]
bldg_panel[
  ,
  market_id := paste(market_zip, year, sep = "_")
]

## Owner ID
if ("owner_mailing" %in% names(bldg_panel)) {
  bldg_panel[
    ,
    owner_mailing_clean := fifelse(
      !is.na(owner_mailing) & nzchar(owner_mailing),
      owner_mailing,
      paste0("UNK_", PID)
    )
  ]
} else if ("owner" %in% names(bldg_panel)) {
  bldg_panel[
    ,
    owner_mailing_clean := fifelse(
      !is.na(owner) & nzchar(owner),
      owner,
      paste0("UNK_", PID)
    )
  ]
} else {
  bldg_panel[
    ,
    owner_mailing_clean := paste0("UNK_", PID)
  ]
}

## ------------------------------------------------------------
## 6) PRODUCT SCOPE & BASIC FILTERS
## ------------------------------------------------------------

## Rental status from ever_rentals_panel columns
bldg_panel[
  ,
  is_rental_year := fifelse(
    fco(ever_rental_any_year, FALSE) |
      fco(rental_from_altos, FALSE)  |
      fco(rental_from_license, FALSE) |
      fco(rental_from_evict, FALSE),
    TRUE, FALSE
  )
]

## Restrict to parcels that ever look like rentals
rental_pids <- bldg_panel[is_rental_year == TRUE, unique(PID)]
bldg_panel  <- bldg_panel[PID %in% rental_pids]

## Restrict to multi-unit rentals (tweak threshold as needed)
#bldg_panel  <- bldg_panel[num_units_imp_final ]

## Drop pre-construction years if you have year_built
if ("year_built" %in% names(bldg_panel)) {
  bldg_panel <- bldg_panel[is.na(year_built) | year >= year_built]
}

## Simple filing rate + high_evict (optional)
if ("num_filings" %in% names(bldg_panel)) {
  bldg_panel[
    ,
    filing_rate := fco(num_filings, 0) / pmax(num_units_imp_final, 1)
  ]
  bldg_panel[
    ,
    high_evict := filing_rate >= 0.1
  ]
}

## ------------------------------------------------------------
## 7) MARKET SHARES & HHI MEASURES
## ------------------------------------------------------------

## Market totals
bldg_panel[
  ,
  total_occupied_units_market := sum(occupied_units, na.rm = TRUE),
  by = .(market_id)
]
bldg_panel[
  ,
  market_share_units := fifelse(
    total_occupied_units_market > 0,
    occupied_units / total_occupied_units_market,
    NA_real_
  )
]

## Owner-market summary
owner_market <- bldg_panel[
  ,
  .(
    owner_units       = sum(occupied_units, na.rm = TRUE),
    owner_parcels     = .N,
    any_evict_owner   = any(fco(rental_from_evict, FALSE) | fco(num_filings, 0) > 0, na.rm = TRUE),
    any_altos_owner   = any(fco(rental_from_altos, FALSE), na.rm = TRUE),
    any_license_owner = any(fco(rental_from_license, FALSE), na.rm = TRUE)
  ),
  by = .(market_id, owner_mailing_clean)
]

market_hhi <- owner_market[
  ,
  {
    total_units <- sum(owner_units, na.rm = TRUE)
    share       <- if (total_units > 0) owner_units / total_units else rep(NA_real_, .N)

    total_parcels <- sum(owner_parcels, na.rm = TRUE)
    share_parcel  <- if (total_parcels > 0) owner_parcels / total_parcels else rep(NA_real_, .N)

    hhi_all   <- sum(share^2, na.rm = TRUE)
    hhi_par   <- sum(share_parcel^2, na.rm = TRUE)

    share_evict <- if (total_units > 0) ifelse(any_evict_owner, owner_units, 0) / total_units else rep(NA_real_, .N)
    hhi_evict   <- sum(share_evict^2, na.rm = TRUE)

    share_sorted <- sort(share, decreasing = TRUE)
    top1 <- share_sorted[1]
    top3 <- sum(head(share_sorted, 3))

    .(
      hhi_all_units   = hhi_all,
      hhi_parcels     = hhi_par,
      hhi_evict_units = hhi_evict,
      share_top1      = top1,
      share_top3      = top3,
      n_owners        = .N
    )
  },
  by = market_id
]

bldg_panel <- merge(
  bldg_panel,
  market_hhi,
  by    = "market_id",
  all.x = TRUE
)

## Unit-bin specific share (optional)
bldg_panel[
  ,
  num_units_bin := cut(
    num_units_imp_final,
    breaks = c(-Inf, 1, 5, 20, 50, Inf),
    labels = c("1", "2-5", "6-20", "21-50", "51+"),
    include.lowest = TRUE
  )
]

bldg_panel[
  ,
  total_occupied_units_bin := sum(occupied_units, na.rm = TRUE),
  by = .(market_id, num_units_bin)
]
bldg_panel[
  ,
  share_units_zip_unit := fifelse(
    total_occupied_units_bin > 0,
    occupied_units / total_occupied_units_bin,
    NA_real_
  )
]

## ------------------------------------------------------------
## 8) SIMPLE BLP-STYLE INSTRUMENTS
## ------------------------------------------------------------

make_blp_instruments <- function(
    DT,
    market,
    product_id,
    firm,
    cont_vars = character(),
    prefix = "z"
) {
  stopifnot(data.table::is.data.table(DT))
  mkt <- market
  frm <- firm

  ## Counts
  DT[, `:=`(
    z_cnt_market = .N,
    z_cnt_firm   = .N
  ), by = c(mkt, frm)]

  DT[
    ,
    `:=`(
      z_cnt_others    = z_cnt_market - 1L,
      z_cnt_samefirm  = z_cnt_firm - 1L,
      z_cnt_otherfirm = z_cnt_market - z_cnt_firm
    )
  ]

  for (v in cont_vars) {
    if (!v %in% names(DT)) {
      warning("make_blp_instruments: skipping missing variable: ", v)
      next
    }

    nm_all <- paste(prefix, "sum_others",    v, sep = "_")
    nm_sf  <- paste(prefix, "sum_samefirm",  v, sep = "_")
    nm_of  <- paste(prefix, "sum_otherfirm", v, sep = "_")

    DT[, tmp_v := fco(get(v), 0)]

    DT[
      ,
      sum_market := sum(tmp_v),
      by = c(mkt)
    ]
    DT[
      ,
      sum_firm := sum(tmp_v),
      by = c(mkt, frm)
    ]
    DT[,(nm_all) := sum_market - tmp_v]
    DT[,(nm_sf)  := sum_firm   - tmp_v]
    DT[,(nm_of)  := sum_market - sum_firm]

  }

  DT[, c("tmp_v", "sum_market", "sum_firm") := NULL]
  invisible(DT)
}

blp_cont_vars <- c(
  "num_units_imp_final",
  "log_med_rent",
  "hazardous_violation_count",
  "total_violations",
  "building_permit_count",
  "general_permit_count",
  "mechanical_permit_count",
  "zoning_permit_count",
  "plumbing_permit_count",
  "taxable_value",
  "change_taxable_value",
  "log_taxable_value",
  "change_taxable_value_per_unit",
  "log_taxable_value_per_unit"
)

bldg_panel <- make_blp_instruments(
  DT         = bldg_panel,
  market     = "market_id",
  product_id = "PID",
  firm       = "owner_mailing_clean",
  cont_vars  = blp_cont_vars,
  prefix     = "z"
)

## ------------------------------------------------------------
## 9) FILTER DOWN TO ANALYTIC SAMPLE
## ------------------------------------------------------------

analytic <- bldg_panel[
  !is.na(log_med_rent) &
    !is.na(market_share_units) &
    !is.na(num_units_imp_final)
]

market_stats <- analytic[
  ,
  .(
    n_products = .N,
    n_owners   = uniqueN(owner_mailing_clean)
  ),
  by = market_id
]

good_markets <- market_stats[
  n_products >= 5 & n_owners >= 2,
  market_id
]

analytic <- analytic[market_id %in% good_markets]
analytic <- analytic[year >= 2006]  ## tweak time window if needed

## ------------------------------------------------------------
## 10) EXPORT
## ------------------------------------------------------------

fwrite(bldg_panel, out_bldg_panel)
fwrite(analytic,   out_analytic)

message("[analytic] bldg_panel rows: ", nrow(bldg_panel))
message("[analytic] analytic_sample rows: ", nrow(analytic))
message("[analytic] unique markets: ", analytic[, uniqueN(market_id)])
message("[analytic] unique owners: ", analytic[, uniqueN(owner_mailing_clean)])
#
bldg_panel = fread(out_bldg_panel)

analytic = fread(out_analytic)

# make sample of apartments with >= 20 units; get median rent, address, num filings
analytic[,num_unique_num_units := uniqueN(num_units_imp_final), by=.(PID)]
large_apts = analytic[num_units_imp_final >= 20, .(median_rent = max(med_rent, na.rm=TRUE),
                                                   year = round(max(year)),
                                                   n_sn_ss_c = first(n_sn_ss_c),
                                                   pm.zip = first(pm.zip),
                                                   census_tract = first(census_tract),
                                                   GEOID = first(GEOID),
                                                   num_units_imp_final = first(num_units_imp_final),
                                         total_filings = sum(fco(num_filings,0), na.rm=TRUE)),
                      by=.(PID)]

fwrite(large_apts, file.path(PROC, "large_apartments.csv"))
places_summary <- fread("output/places_summary.csv")
places_summary[,n_sn_ss_c := str_match(text_search_query, "^([0-9a-zA-Z\\s-]+)(, phila)")[,2]]

# laod json review data
json_file = "output/reviews_raw.ndjson"
library(ndjson)
reviews_data = ndjson::stream_in(json_file)
# flag review as containing "fake"
setDT(reviews_data)
reviews_data[, fake_review := grepl("(fake|fraud|scam).+review", tolower(text))]
reviews_data[,building_has_fake_reviews := any(fake_review), by=.(place_id)]
reviews_data[,most_recent_review := case_when(
  str_detect(relative_time_description, "last week|months ago") ~ "within last year",
  str_detect(relative_time_description, "a year ago") ~ "over a year",
  TRUE ~ "over two years"
)]
large_apts_places <- merge(analytic, places_summary,
                          by = "n_sn_ss_c",
                           all.x = F)
setorder(reviews_data, "place_id", "time_unix")
reviews_data_bldg_agg = reviews_data[, .(building_has_fake_reviews = any(building_has_fake_reviews),
                                         most_recent_review = first(most_recent_review)),
                                      by=.(place_id)]
#
large_apts_places = merge(large_apts_places, reviews_data_bldg_agg,
                          by.x = "place_id", by.y = "place_id",
                          all.x = TRUE)

large_apts_places[,filing_rate_binned := cut(filing_rate,
                                        breaks = c(-Inf, 0, 0.05, 0.1, Inf),
                                        labels = c("0", "0-0.05", "0.05-0.1", "0.1+"),
                                        include.lowest = TRUE)]

# bin ratings into half-stars
large_apts_places[,rating_round := round(rating * 2) / 2]

large_apts_places[,year_built_decade := cut(year_built,
                                      breaks = c(-Inf, 1900, 1910, 1920, 1930, 1940, 1950, 1960, 1970, 1980, 1990, 2000, 2010, Inf),
                                      labels = c("<1900", "1900s", "1910s", "1920s", "1930s", "1940s", "1950s", "1960s", "1970s", "1980s", "1990s", "2000s", "2010s+"),
                                      include.lowest = TRUE)]
large_apts_places[,num_units_cuts := cut(num_units_imp_final,
                                 breaks = c(19, 50, 100, 200, 500, Inf),
                                 labels = c("20-50", "51-100", "101-200", "201-500", "500+"),
                                 include.lowest = TRUE)]
feols(log_med_rent ~ rating + log(user_ratings_total) + i(filing_rate_binned, ref = '0.05-0.1')   | year_built_decade+GEOID +  year +num_units_cuts +quality_grade +building_code_description_new_fixed ,
      weights = ~num_units_imp_final,
      cluster = ~PID,
      data = large_apts_places[ year >= 2010 & filing_rate <= 1 & user_ratings_total > 10 & user_ratings_total < 100 & num_units_imp_final > 0]) |> summary()

feols(log_med_rent ~i(rating_round, ref = 3.5)  |year ,
      weights = ~num_units_imp_final,
      cluster = ~PID,
      data = large_apts_places[ year >= 2010  & filing_rate <= 1 & user_ratings_total > 10 & user_ratings_total < 100 & num_units_imp_final > 0]) |> summary()

# residualize out year fe
library(fixest)
large_apts_places[num_units_imp_final > 0,log_med_rent_resid := resid(fixest::feols(log_med_rent ~1  | year,
                                               weights = ~num_units_imp_final,
                                               cluster = ~PID,
                                               data = large_apts_places[num_units_imp_final > 0]))]

# ggplot rating vs median rent; size by user_ratings_total
ggplot(large_apts_places[user_ratings_total > 10 & user_ratings_total < 300 & rating >1.5 & year >= 2011],
       aes(x = rating, y = log_med_rent_resid, size = user_ratings_total, group = filing_rate_binned)) +
  geom_point(alpha = 0.6) +
  geom_smooth(method = "lm", se = TRUE, color = "blue") +
  scale_x_continuous(breaks = seq(2, 5, by = 0.5)) +
  labs(title = "Median Rent vs. Apartment Rating",
       x = "Apartment Rating",
       y = "Median Rent") +
  theme_minimal() +
  facet_wrap(~filing_rate_binned)

# do this in feols
feols(log_med_rent ~  rating_round : filing_rate_binned + filing_rate_binned  | year,
      #weights = ~num_units_imp_final,
      cluster = ~PID,
      data = large_apts_places[  filing_rate <= 1 & user_ratings_total > 10 & user_ratings_total < 100 & num_units_imp_final > 0]) |> summary()

# mean rent by rating_round
mean_rent_by_rating <- large_apts_places[user_ratings_total > 10, .(mean_rent = mean(med_rent, na.rm=TRUE),
                                              median_rent = median(med_rent, na.rm=TRUE),
                                              n_apartments = uniqueN(PID)),
                                         by=.(rating_round)]
mean_rent_by_rating[order(rating_round)]

