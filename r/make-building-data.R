#### libraries ####
library(data.table)
library(tidyverse)
library(postmastr)
library(bit64)
library(glue)
library(janitor)

print("loaded libraries")

#### ---------- SETTINGS ----------- ####
# Choose: "year", "quarter", or "month"
agg_level <- "quarter"   # <- change here

# Optional: period domain bounds (used if you want to override min/max from data)
domain_start <- as.Date("2005-01-01")
domain_end   <- as.Date("2025-12-31")

#### directories ####
print("setting directories")
input_dir  = "~/Desktop/data/philly-evict/open-data"
output_dir = "~/Desktop/data/philly-evict/open-data"

setwd(input_dir)
permits        = fread("permits.csv")
complaints     = fread("complaints.csv")
violations     = fread("violations.csv")
investigations = fread("case_investigations.csv")
parcels        = fread("/Users/joefish/Desktop/data/philly-evict/parcel_address_cleaned.csv")

# ---------- Helpers: date -> period ----------
as_date_safe <- function(x) {
  # Try to coerce to Date; if not, take first 10 chars
  # Works for "YYYY-MM-DD ...", "YYYY/MM/DD", etc.
  y <- suppressWarnings(as.IDate(x))
  if (all(is.na(y))) {
    y <- suppressWarnings(as.IDate(substr(x, 1, 10)))
  }
  return(as.Date(y))
}

to_period <- function(date_vec, level = c("year","quarter","month")) {
  level <- match.arg(level)
  d <- as_date_safe(date_vec)
  mm <- as.integer(format(d, "%m"))
  yy <- format(d, "%Y")
  q  <- ((mm - 1L) %/% 3L) + 1L
  out <-
    switch(level,
           "year"    = yy,
           "quarter" = paste0(yy, "-Q", q),
           "month"   = format(d, "%Y-%m"))
  return(out)
}

# Build a complete sequence of period labels for a domain
period_seq <- function(start_date, end_date, level = c("year","quarter","month")) {
  level <- match.arg(level)
  start_date <- as.Date(start_date)
  end_date   <- as.Date(end_date)
  if (level == "year") {
    yrs <- seq(as.integer(format(start_date,"%Y")), as.integer(format(end_date,"%Y")))
    return(as.character(yrs))
  } else if (level == "quarter") {
    # monthly seq, then label by quarter and unique
    months <- seq(from = as.Date(format(start_date,"%Y-%m-01")),
                  to   = as.Date(format(end_date,  "%Y-%m-01")),
                  by = "1 month")
    labs <- to_period(months, "quarter")
    return(unique(labs))
  } else {
    months <- seq(from = as.Date(format(start_date,"%Y-%m-01")),
                  to   = as.Date(format(end_date,  "%Y-%m-01")),
                  by = "1 month")
    return(format(months, "%Y-%m"))
  }
}

# ---------- PERMITS ----------
permits[, period := to_period(permitissuedate, agg_level)]

clean_permit_type <- function(permit_type) {
  pt <- toupper(trimws(permit_type))
  dplyr::case_when(
    pt %in% c("ELECTRICAL", "EP_ELECTRL") ~ "Electrical",
    pt %in% c("MECHANICAL", "BP_MECH") ~ "Mechanical",
    pt %in% c("RESIDENTIAL BUILDING", "BP_NEWCNST") ~ "Residential Building",
    pt %in% c("FIRE SUPPRESSION", "BP_FIRESUP") ~ "Fire Suppression",
    pt %in% c("BUILDING", "BP_ALTER", "BP_ADDITON", "BP_SIGN", "BP_DEMO", "BP_ADMINST") ~ "Building",
    pt %in% c("PLUMBING", "PP_PLUMBNG") ~ "Plumbing",
    pt %in% c("ZONING", "ZP_USE", "ZP_ZON/USE", "ZP_ZONING", "ZP_ADMIN") ~ "Zoning",
    pt %in% c("SITE / UTILITY PERMIT") ~ "Site/Utility Permit",
    pt %in% c("DEMOLITION") ~ "Demolition",
    pt %in% c("ADMINISTRATIVE") ~ "Administrative",
    pt %in% c("MASTER PLAN") ~ "Master Plan",
    pt %in% c("GENERAL", "GENERAL PERMIT MINOR") ~ "General",
    pt %in% c("OPERATIONS", "OPS PERMIT") ~ "Operations",
    pt %in% c("TANK TEST") ~ "Tank Test",
    pt %in% c("L_FFORM") ~ "L_FFORM",
    TRUE ~ pt
  )
}
permits[, permit_standardized := clean_permit_type(permittype)]
valid_permits <- permits[ , .N, by = permit_standardized][order(-N)][N > 10000, permit_standardized]

permits_agg_long <- permits[
  permit_standardized %in% valid_permits,
  .(num_permits = .N),
  by = .(parcel_number = opa_account_num, period, permit_standardized)
]
permits_agg_long[ , total_permits := sum(num_permits), by = .(parcel_number, period)]

permits_agg <- pivot_wider(
  permits_agg_long,
  names_from  = permit_standardized,
  values_from = num_permits,
  values_fill = 0
) |> janitor::clean_names() |> as.data.table()

# ---------- COMPLAINTS ----------
complaint_lookup <- tribble(
  ~raw, ~clean,
  "EMERGENCY",                                                "Emergency/Service",
  "BUILDING CONSTRUCTION",                                    "Building",
  "VACANT HOUSE",                                             "Vacant Property",
  "DRAINAGE MAINTENANCE RESIDENTIAL TO HCEU",                 "Drainage",
  "NO HEAT",                                                  "Heat",
  "BUILDING DANGEROUS",                                       "Building",
  "MAINTENANCE RESIDENTIAL",                                  "Property Maintenance",
  "ZONING RESIDENTIAL",                                       "Zoning",
  "MAINTENANCE COMMERCIAL",                                   "Property Maintenance",
  "ELECTRICAL CONSTRUCTION",                                  "Electrical",
  "VACANT LOT RESIDENTIAL",                                   "Vacant Property",
  "COMPLAINTS ASSOCIATED WITH DEMOS",                         "Demolition",
  "OTHER DANGEROUS OCCUPIED",                                 "Other",
  "INFESTATION RESIDENTIAL",                                  "Infestation",
  "COMMUNITY LIFE IMPROVEMENT PROGRAM",                       "Program/Initiative",
  "FIRE RESIDENTIAL",                                         "Fire",
  "ZONING BUSINESS",                                          "Zoning",
  "NEIGHBORHOOD SERVICES BLIGHT REMOVAL",                     "Blight Removal",
  "LICENSE BUSINESS",                                         "License/Business",
  "SERVICES & OPERATIONS CLOSE",                              "Administrative/Service",
  "BUILDING DANGEROUS OCCUPIED",                              "Building",
  "FIRE COMMERCIAL",                                          "Fire",
  "VACANT COMMERCIAL",                                        "Vacant Property",
  "TREE DANGEROUS OCCUPIED",                                  "Tree",
  "DAY CARE COMMERCIAL",                                      "Daycare",
  "HAZARDOUS MATERIAL",                                       "Hazardous Material",
  "SIGNAGE",                                                  "Signage",
  "ZONING CONSTRUCTION",                                      "Zoning",
  "WEIGHTS & MEASURES COMPLAINTS",                            "Weights & Measures",
  "PLUMBING CONSTRUCTION",                                    "Plumbing",
  "TREE DANGEROUS",                                           "Tree",
  "BOARDING OR ROOMING HOUSE",                                "Boarding/Rooming House",
  "OTHER DANGEROUS",                                          "Other",
  "NUISANCE TASK FORCE",                                      "Nuisance",
  "PROJECT",                                                  "Administrative/Project",
  "VACANT LOT COMMERCIAL",                                    "Vacant Property",
  "DRAINAGE MAINTENANCE COMMERCIAL TO CI",                    "Drainage",
  "LICENSE CONSTRUCTION",                                     "License/Business",
  "EXTERIOR COMPLAINT",                                       "Property Maintenance",
  "CONTRACTUAL SERVICES VIOLATIONS",                          "Administrative/Service",
  "DAY CARE RESIDENTIAL",                                     "Daycare",
  "SCRAP YARD",                                               "Nuisance Business",
  "TRASH OR DEBRIS RESIDENTIAL",                              "Trash/Weeds",
  "HEAT COMPLAINTS IN COMMERCIAL BUILDING",                   "Heat",
  "VACANT LOT",                                               "Vacant Property",
  "DRAINAGE COMPLAINT",                                       "Drainage",
  "VACANT LOTS (CLIP)",                                       "Vacant Property",
  "SERVICE CALL",                                             "Administrative/Service",
  "BUILDING DANGEROUS SINKING",                               "Building",
  "VACANT BUILDING COMMERCIAL",                               "Vacant Property",
  "ILLEGAL BUSINESS",                                         "License/Business",
  "OVERCROWDED - COMMERCIAL",                                 "Property Maintenance",
  "VACANT HOUSE RESIDENTIAL",                                 "Vacant Property",
  "BUGS OR MICE",                                             "Infestation",
  "ELECTRICAL WORK IN PROGRESS WITHOUT PERMIT",               "Work Without/Permit Violation",
  "FREE STANDING RETAINING WALL FALLING",                     "Structural/Retaining Wall",
  "WATER IN BASEMENT RESIDENTIAL",                            "Water Intrusion",
  "ROOF LEAK RESIDENTIAL",                                    "Roof Leak",
  "CONSTRUCTION IN PROGRESS WITHOUT PERMIT",                  "Work Without/Permit Violation",
  "HIGH GRASS OR WEEDS RESIDENTIAL",                          "Trash/Weeds",
  "TRASH OR DEBRIS COMMERCIAL",                               "Trash/Weeds",
  "DECK CONSTRUCTION",                                        "Building",
  "TREE FALLING SPECIFY AREA OF PROPERTY",                    "Tree",
  "LEAD ABATEMENT HEALTH DEPT",                               "Lead Abatement (Health)",
  "STAGNANT POOL WATER",                                      "Standing Water",
  "REFERRAL",                                                 "Administrative",
  "NO RENTAL LICENSE",                                        "License/Business",
  "SIGN ILLEGAL",                                             "Signage",
  "OCCUPIED RESIDENCE WITHOUT HEAT",                          "Heat",
  "BUILDING FALLING - PLEASE DESCRIBE",                       "Building",
  "ZONING MISCELLANEOUS",                                     "Zoning",
  "SPECIAL BUILDING DANGEROUS",                               "Building",
  "BLOCKED DRAIN RESIDENTIAL",                                "Drainage",
  "ROOF LEAK COMMERCIAL",                                     "Roof Leak",
  "BUILDING DANGEROUS HISTORICAL",                            "Building",
  "REDEVELOPMENT AUTHORITY - VACANT HOUSE CONDEMNATION",      "Vacant Property",
  "NUISANCE PROPERTY UNIT",                                   "Nuisance",
  "LICENSE",                                                  "License/Business",
  "SPECIAL VACANT HOUSE",                                     "Vacant Property",
  "SPECIAL BUILDING CONSTRUCTION",                            "Building",
  "SPECIAL VACANT LOT RESIDENTIAL",                           "Vacant Property",
  "SPECIAL MAINTENANCE RESIDENTIAL",                          "Property Maintenance",
  "OTHER",                                                    "Other",
  "LICENSE RESIDENTIAL",                                      "License/Business",
  "SERVICE REQUEST FROM 3-1-1",                               "Administrative/Service",
  "COMPLIANCE AUDIT",                                         "Compliance/Inspection",
  "BLOCKED",                                                  "Drainage",
  "HIGH GRASS OR WEEDS COMMERCIAL",                           "Trash/Weeds",
  "DAYCARE CENTER COMMERCIAL",                                "Daycare",
  "SPECIAL MAINTENANCE COMMERCIAL",                           "Property Maintenance",
  "SPECIAL CIRCUMSTANCE",                                     "Administrative",
  "PLUMBING WORK IN PROGRESS WITHOUT PERMIT",                 "Work Without/Permit Violation",
  "SPECIAL VACANT LOT COMMERCIAL",                            "Vacant Property",
  "DAYCARE RESIDENTIAL",                                      "Daycare",
  "CONSTRUCTION SITE TASK FORCE",                             "Building",
  "ASSIGN DYNAMIC PORTAL",                                    "Administrative",
  "WATER IN BASEMENT COMMERCIAL",                             "Water Intrusion",
  "VACANT STRATEGY INSPECTIONS",                              "Vacant Property",
  "SPECIAL PLUMBING CONSTRUCTION",                            "Plumbing",
  "SPECIAL HAZARDOUS MATERIAL",                               "Hazardous Material",
  "RESIDENTIAL STRUCTURAL ASSESSMENT",                        "Structural Assessment",
  "LARGE VACANT",                                             "Vacant Property",
  "BLOCKED DRAIN COMMERCIAL",                                 "Drainage",
  "INSTALLATION OF POOL WITHOUT PERMIT",                      "Work Without/Permit Violation",
  "VENDOR COMPLAINT",                                         "Vendor Complaint",
  "ALL PENN ISSUES WILL BE ASSIGNED TO HCEU",                 "Administrative",
  "PROPERTY MAINTENANCE COMPLAINT INTERIOR",                  "Property Maintenance",
  "FIRE SAFETY COMPLAINT",                                    "Fire",
  "VACANT PROPERTY COMPLAINT",                                "Vacant Property",
  "UNLICENSED COMMERCIAL BUSINESS (FOOD, SOAL, AUTO REPAIR)", "License/Business",
  "PROPERTY MAINTENANCE HIGH WEEDS",                          "Trash/Weeds",
  "WORK UNDERWAY WITHOUT PERMITS",                            "Work Without/Permit Violation",
  "RESIDENCE WITHOUT HEAT (OCTOBER 1 - APRIL 15)",            "Heat",
  "WORK UNDERWAY IN VIOLATION OF PERMIT REQUIREMENTS",        "Work Without/Permit Violation",
  "PROPERTY MAINTENANCE COMPLAINT EXTERIOR",                  "Property Maintenance",
  "BUILDING STRUCTURALLY DEFICIENT (OCCUPIED)",               "Structural Deficiency",
  "BUILDING STRUCTURALLY DEFICIENT (NOT OCCUPIED)",           "Structural Deficiency",
  "PROPERTY MAINTENANCE EXTERIOR REVIEWED",                   "Property Maintenance",
  "FIRE SAFETY COMPLAINT MULTI FAMILY DWELLING",              "Fire",
  "GRAFFITI",                                                 "Graffiti",
  "BROKEN MAIN DRAIN/RAW SEWAGE WITHIN THE PROPERTY",         "Sewage/Sanitary",
  "DEMOLITION COMPLAINTS",                                    "Demolition",
  "FIRE SAFETY COMPLAINT ASSEMBLY SPACE",                     "Fire",
  "FIRE SAFETY COMPLAINT HIGH RISE",                          "Fire",
  "ENCROACHING TREES",                                        "Tree",
  "SHORT TERM RENTAL COMPLAINT",                              "Short-Term Rental",
  "PLASTIC BAG COMPLAINT",                                    "Plastic Bag",
  "NUISANCE BUSINESS COMPLAINT",                              "Nuisance Business",
  "BUILDING CERT DEFICIENCY",                                 "Structural Deficiency",
  "VACANT & OPEN TO TRESPASS",                                "Vacant Property",
  "BOARDING/ROOMING HOUSE",                                   "Boarding/Rooming House",
  "CLEAN & GREEN INITIATIVE",                                 "Program/Initiative",
  "FIRE SAFETY COMPLAINT SINGLE/DOUBLE FAMILY",               "Fire",
  "UNLICENSED RENTALS",                                       "License/Business",
  "FIRE SAFETY COMPLAINT COMMERCIAL BUSINESS",                "Fire",
  "ZONING COMMERCIAL BUSINESS",                               "Zoning",
  "COMMISSIONER SPECIALS",                                    "Administrative"
)

clean_complaint_type <- function(x, lookup = complaint_lookup) {
  key_dt <- as.data.table(lookup)[ , .(key = toupper(trimws(raw)), clean)]
  inp <- data.table(i = seq_along(x), raw = x)[ , key := toupper(trimws(raw))]
  res <- key_dt[inp, on = .(key), nomatch = NA][order(i)][ , ifelse(is.na(clean), raw, clean)]
  return(res)
}

complaints[, period := to_period(complaintdate, agg_level)]
complaints[, complaint_type_standaridzed := clean_complaint_type(complaintcodename)]
valid_complaints <- complaints[ , .N, by = complaint_type_standaridzed][order(-N)][N > 10000, complaint_type_standaridzed]

complaints_agg_long <- complaints[
  complaint_type_standaridzed %in% valid_complaints,
  .(num_complaints = .N),
  by = .(parcel_number = opa_account_num, period, complaint_type_standaridzed)
]
complaints_agg_long[ , total_complaints := sum(num_complaints), by = .(parcel_number, period)]

complaints_agg <- pivot_wider(
  complaints_agg_long,
  names_from  = complaint_type_standaridzed,
  values_from = num_complaints,
  values_fill = 0
) |> janitor::clean_names() |> as.data.table()

# ---------- VIOLATIONS ----------
violations[, period := to_period(violationdate, agg_level)]
violations[, violation_standardized := fifelse(
  caseprioritydesc == "", "OTHER", toupper(trimws(caseprioritydesc))
)]
valid_violations <- violations[ , .N, by = violation_standardized][order(-N)][N > 10000, violation_standardized]

violations_agg_long <- violations[
  violation_standardized %in% valid_violations,
  .(num_violations = .N),
  by = .(parcel_number = opa_account_num, period, violation_standardized)
]
violations_agg_long[ , total_violations := sum(num_violations), by = .(parcel_number, period)]

violations_agg <- pivot_wider(
  violations_agg_long,
  names_from  = violation_standardized,
  values_from = num_violations,
  values_fill = 0
) |> janitor::clean_names() |> as.data.table()

# ---------- INVESTIGATIONS ----------
investigations[, period := to_period(investigationcompleted, agg_level)]
investigations[, investigation_standardized := fifelse(
  casepriority == "", "OTHER", toupper(trimws(casepriority))
)]
valid_investigations <- investigations[ , .N, by = investigation_standardized][order(-N)][N > 10000, investigation_standardized]

investigations_agg_long <- investigations[
  investigation_standardized %in% valid_investigations,
  .(num_investigations = .N),
  by = .(parcel_number = opa_account_num, period, investigation_standardized)
]
investigations_agg_long[ , total_investigations := sum(num_investigations), by = .(parcel_number, period)]

investigations_agg <- pivot_wider(
  investigations_agg_long,
  names_from  = investigation_standardized,
  values_from = num_investigations,
  values_fill = 0
) |> janitor::clean_names() |> as.data.table()

# ---------- Build parcel x period grid ----------
# Determine min/max dates across data (fallback to domain bounds)
mind <- min(
  as_date_safe(permits$permitissuedate),
  as_date_safe(complaints$complaintdate),
  as_date_safe(violations$violationdate),
  as_date_safe(investigations$investigationcompleted),
  na.rm = TRUE
)
maxd <- max(
  as_date_safe(permits$permitissuedate),
  as_date_safe(complaints$complaintdate),
  as_date_safe(violations$violationdate),
  as_date_safe(investigations$investigationcompleted),
  na.rm = TRUE
)

if (!is.finite(mind)) mind <- domain_start
if (!is.finite(maxd)) maxd <- domain_end

all_periods <- period_seq(start_date = min(domain_start, mind),
                          end_date   = max(domain_end,   maxd),
                          level = agg_level)

parcel_grid <- expand_grid(
  parcel_number = unique(parcels$parcel_number),
  period = all_periods
) |> as.data.table()

# ---------- Suffix & merges ----------
permits_agg <- permits_agg |>
  rename_with(~paste0(.x, "_permit_count"), -c(parcel_number, period, total_permits))

complaints_agg <- complaints_agg |>
  rename_with(~paste0(.x, "_complaint_count"), -c(parcel_number, period, total_complaints))

violations_agg <- violations_agg |>
  rename_with(~paste0(.x, "_violation_count"), -c(parcel_number, period, total_violations))

investigations_agg <- investigations_agg |>
  rename_with(~paste0(.x, "_investigation_count"), -c(parcel_number, period, total_investigations))

building_data <- parcel_grid |>
  merge(permits_agg,        by = c("parcel_number","period"), all.x = TRUE) |>
  merge(complaints_agg,     by = c("parcel_number","period"), all.x = TRUE) |>
  merge(violations_agg,     by = c("parcel_number","period"), all.x = TRUE) |>
  merge(investigations_agg, by = c("parcel_number","period"), all.x = TRUE)

# Fill NAs with zeros
building_data <- building_data |>
  mutate(across(total_permits:last_col(), ~replace_na(.x, 0)))

# Optional: summary by period for quick QC
building_data[ , .(
  total_permits        = sum(total_permits,        na.rm = TRUE),
  total_complaints     = sum(total_complaints,     na.rm = TRUE),
  total_violations     = sum(total_violations,     na.rm = TRUE),
  total_investigations = sum(total_investigations, na.rm = TRUE)
), by = period][order(period)] |> print()

# Derive year integer (useful even for month/quarter outputs)
building_data[ , year := as.integer(substr(period, 1, 4))]

# ---------- Write ----------
outfile <- glue("{output_dir}/building_data_{agg_level}.csv")
fwrite(building_data, file = outfile, row.names = FALSE, na = "NA")
print(glue("Wrote {outfile}"))
