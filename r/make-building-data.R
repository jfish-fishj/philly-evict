#### libraries ####
library(data.table)
library(tidyverse)
library(postmastr)
library(bit64)
library(glue)
library(janitor)
library(glue)
print("loaded libraries")

#### directories ####
print("setting directories")
input_dir = "~/Desktop/data/philly-evict/open-data"
output_dir = "~/Desktop/data/philly-evict/open-data"

setwd(input_dir)
permits = fread("permits.csv")
complaints = fread("complaints.csv")
violations = fread("violations.csv")
investigations = fread("case_investigations.csv")
parcels = fread("/Users/joefish/Desktop/data/philly-evict/parcel_address_cleaned.csv")



# make parcel by year level aggregates of each data set
#### permit aggs ####
permits[,year := as.numeric(str_sub(permitissuedate,1,4))]

clean_permit_type <- function(permit_type) {
  # Convert to upper case, trim whitespace
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
    pt %in% c("L_FFORM") ~ "L_FFORM",  # unsure category
    TRUE ~ pt  # keep original if no match
  )
}
permits[,permit_standardized := clean_permit_type(permittype)]

valid_permits <- permits[,.N, by = permit_standardized][order(-N)][N > 10000,permit_standardized]
permits_agg_long = permits[permit_standardized %in% valid_permits,list(
  num_permits = .N
), by = .(parcel_number = opa_account_num, year,permit_standardized)]

permits_agg_long[,total_permits := sum(num_permits), by = .(parcel_number, year)]
permits_agg = pivot_wider(permits_agg_long,
                          names_from = permit_standardized,
                          values_from = num_permits,
                          values_fill = 0) %>%
  janitor::clean_names()
setDT(permits_agg)

#### complaint aggs ####
complaints[,year := as.numeric(str_sub(complaintdate,1,4))]
# standardize permits
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

# Helper: clean a vector using the table (case-insensitive, trims)

clean_complaint_type <- function(x, lookup = complaint_lookup) {
  # lookup: data.frame/tibble/data.table with columns `raw` and `clean`
  key_dt <- as.data.table(lookup)[
    , .(key = toupper(trimws(raw)), clean)
  ]
  inp <- data.table(i = seq_along(x), raw = x)[
    , key := toupper(trimws(raw))
  ]
  # Left-join by normalized key, preserving input order (order of i)
  res <- key_dt[inp, on = .(key), nomatch = NA
  ][order(i)][
    , ifelse(is.na(clean), raw, clean)]
  return(res)
}

complaints[,complaint_type_standaridzed := clean_complaint_type(complaintcodename)]
valid_complaints <- complaints[,.N, by = complaint_type_standaridzed][order(-N)][N > 10000,complaint_type_standaridzed]
complaints_agg_long = complaints[complaint_type_standaridzed %in% valid_complaints,list(
  num_complaints = .N
), by = .(parcel_number = opa_account_num, year, complaint_type_standaridzed)]

complaints_agg_long[,total_complaints := sum(num_complaints), by = .(parcel_number, year)]
complaints_agg = pivot_wider(complaints_agg_long,
                          names_from = complaint_type_standaridzed,
                          values_from = num_complaints,
                          values_fill = 0) %>%
  janitor::clean_names()

setDT(complaints_agg)

#### violation aggs ####
violations[,year := as.numeric(str_sub(violationdate,1,4))]
# standardize violations
violations[,violation_standardized := fifelse(
  caseprioritydesc == "",
  "OTHER",
  toupper(trimws(caseprioritydesc))
  )]
valid_violations <- violations[,.N, by = violation_standardized][order(-N)][N > 10000,violation_standardized]
violations_agg_long = violations[violation_standardized %in% valid_violations,list(
  num_violations = .N
), by = .(parcel_number = opa_account_num, year, violation_standardized)]
violations_agg_long[,total_violations := sum(num_violations), by = .(parcel_number, year)]
violations_agg = pivot_wider(violations_agg_long,
                          names_from = violation_standardized,
                          values_from = num_violations,
                          values_fill = 0) %>%
  janitor::clean_names()
setDT(violations_agg)

#### investigation aggs ####
# works same as violations so just reuse that code
investigations[,year := as.numeric(str_sub(investigationcompleted,1,4))]
investigations[,investigation_standardized := fifelse(
  casepriority == "",
  "OTHER",
  toupper(trimws(casepriority))
)]
valid_investigations <- investigations[,.N, by = investigation_standardized][order(-N)][N > 10000,investigation_standardized]
investigations_agg_long = investigations[investigation_standardized %in% valid_investigations,list(
  num_investigations = .N
), by = .(parcel_number = opa_account_num, year, investigation_standardized)]
investigations_agg_long[,total_investigations := sum(num_investigations), by = .(parcel_number, year)]
investigations_agg = pivot_wider(investigations_agg_long,
                          names_from = investigation_standardized,
                          values_from = num_investigations,
                          values_fill = 0) %>%
  janitor::clean_names()
setDT(investigations_agg)

#### merge together ####
parcel_grid = expand_grid(
  parcel_number = unique(parcels$parcel_number),
  year = 2005:2025
) %>% as.data.table()

# rename across each dataframe, appending a suffix to each column
permits_agg = permits_agg %>%
  rename_with(~paste0(.x, "_permit_count"), -c(parcel_number, year, total_permits))

complaints_agg = complaints_agg %>%
  rename_with(~paste0(.x, "_complaint_count"), -c(parcel_number, year, total_complaints))

violations_agg = violations_agg %>%
  rename_with(~paste0(.x, "_violation_count"), -c(parcel_number, year, total_violations))

investigations_agg = investigations_agg %>%
  rename_with(~paste0(.x, "_investigation_count"), -c(parcel_number, year, total_investigations))

building_data = parcel_grid %>%
  merge(permits_agg, by = c("parcel_number", "year"), all.x = TRUE) %>%
  merge(complaints_agg, by = c("parcel_number", "year"), all.x = TRUE) %>%
  merge(violations_agg, by = c("parcel_number", "year"), all.x = TRUE) %>%
  merge(investigations_agg, by = c("parcel_number", "year"), all.x = TRUE)

# fill NAs w/ zeros
building_data <- building_data %>%
  mutate(across(total_permits:last_col(), ~replace_na(.x, 0)))

building_data[,list(
  total_permits = sum(total_permits, na.rm = TRUE),
  total_complaints = sum(total_complaints, na.rm = TRUE),
  total_violations = sum(total_violations, na.rm = TRUE),
  total_investigations = sum(total_investigations, na.rm = TRUE)
), by = year]

fwrite(building_data,
       file = glue("{output_dir}/building_data.csv"),
       row.names = FALSE,
       na = "NA")
