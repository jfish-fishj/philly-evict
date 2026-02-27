## ============================================================
## make-rental-building-data.R
## ============================================================
## Purpose: Create parcel-period panels of building permits,
##          complaints, violations, and investigations —
##          restricted to parcels identified as ever-rentals.
##
##          Produces three output files:
##            - building_data_rental_year.parquet
##            - building_data_rental_quarter.parquet
##            - building_data_rental_month.parquet
##
## Inputs:
##   - cfg$inputs$open_data_permits
##   - cfg$inputs$open_data_complaints
##   - cfg$inputs$open_data_violations
##   - cfg$inputs$open_data_case_investigations
##   - cfg$products$ever_rentals_pid  (defines rental universe)
##
## Outputs:
##   - panels/building_data_rental_{year,quarter,month}.parquet
##
## Primary key: (parcel_number, period)
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(bit64)
  library(glue)
  library(janitor)
  library(arrow)
})

# ---- Load config and set up logging ----
source("r/config.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "make-rental-building-data.log")

logf("=== Starting make-rental-building-data.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ---- Settings ----
domain_start <- as.Date("2005-01-01")
domain_end   <- as.Date("2025-12-31")

# ---- Load rental universe ----
rental_pids <- fread(p_product(cfg, "ever_rentals_pid"), select = "PID")
rental_pids <- rental_pids[!is.na(PID)]
setnames(rental_pids, "PID", "parcel_number")
setkey(rental_pids, parcel_number)
logf("Rental parcels: ", nrow(rental_pids), log_file = log_file)

# ---- Load raw open data ----
logf("Loading input data...", log_file = log_file)

permits        <- fread(p_input(cfg, "open_data_permits"))
complaints     <- fread(p_input(cfg, "open_data_complaints"))
violations     <- fread(p_input(cfg, "open_data_violations"))
investigations <- fread(p_input(cfg, "open_data_case_investigations"))

logf("  Permits: ", nrow(permits), " rows", log_file = log_file)
logf("  Complaints: ", nrow(complaints), " rows", log_file = log_file)
logf("  Violations: ", nrow(violations), " rows", log_file = log_file)
logf("  Investigations: ", nrow(investigations), " rows", log_file = log_file)

# ---------- Helpers: date -> period ----------
as_date_safe <- function(x) {
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
  switch(level,
         "year"    = yy,
         "quarter" = paste0(yy, "-Q", q),
         "month"   = format(d, "%Y-%m"))
}

period_seq <- function(start_date, end_date, level = c("year","quarter","month")) {
  level <- match.arg(level)
  start_date <- as.Date(start_date)
  end_date   <- as.Date(end_date)
  if (level == "year") {
    yrs <- seq(as.integer(format(start_date,"%Y")), as.integer(format(end_date,"%Y")))
    return(as.character(yrs))
  } else if (level == "quarter") {
    months <- seq(from = as.Date(format(start_date,"%Y-%m-01")),
                  to   = as.Date(format(end_date,  "%Y-%m-01")),
                  by = "1 month")
    return(unique(to_period(months, "quarter")))
  } else {
    months <- seq(from = as.Date(format(start_date,"%Y-%m-01")),
                  to   = as.Date(format(end_date,  "%Y-%m-01")),
                  by = "1 month")
    return(format(months, "%Y-%m"))
  }
}

# ---------- Permit type standardization ----------
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

# ---------- Complaint type standardization ----------
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
  key_dt <- as.data.table(lookup)[, .(key = toupper(trimws(raw)), clean)]
  inp <- data.table(i = seq_along(x), raw = x)[, key := toupper(trimws(raw))]
  res <- key_dt[inp, on = .(key), nomatch = NA][order(i)][, ifelse(is.na(clean), raw, clean)]
  return(res)
}

severe_complaint_types <- c("Heat", "Fire", "Drainage", "Property Maintenance")

severe_violation_types <- c("HAZARDOUS", "IMMINENTLY DANGEROUS")

investigationtype_lookup <- tribble(
  ~raw,           ~clean,
  "",             "Unknown/Blank",
  "HCEU INSP",    "Code Enforcement (Housing)",
  "PRECOURT",     "Code Enforcement (Pre-Court)",
  "ADMIN INSP",   "Administrative Inspection",
  "CEASE INSP",   "Enforcement (Cease/Stop)",
  "NTF INSP",     "Nuisance Task Force",
  "CSTF INSP",    "Construction Site Task Force",
  "COMP INSP",    "Compliance Inspection",
  "NP INSP",      "Nuisance Property",
  "AIU INSP",     "Audits/Investigations",
  "CI INSP",      "Construction Inspection",
  "CI TANK",      "Construction Inspection (Tank)",
  "CI HAZMAT",    "Hazardous Material",
  "BP_BLDG",      "Building Permit / Building",
  "BP_LICENSE",   "License/Business",
  "EP_ELEC",      "Electrical",
  "PP_PLUMB",     "Plumbing",
  "ZP_ZONING",    "Zoning",
  "SIGN",         "Signage",
  "FACAD INSP",   "Facade/Exterior",
  "ASBESTOS",     "Hazardous Material",
  "POST C&S",     "Construction (Post Clean & Seal)",
  "DEMO PUNCH",   "Demolition",
  "CSUINITIAL",   "Case Support / Utility Inspection",
  "CSUFINAL",     "Case Support / Utility Inspection",
  "CSUCURBINT",   "Case Support / Utility Inspection",
  "CSUSTUBAR",    "Case Support / Utility Inspection",
  "CSUTESTPIT",   "Case Support / Utility Inspection",
  "CSUEQTY",      "Case Support / Utility Inspection",
  "CSUPOSTCS",    "Case Support / Utility Inspection",
  "L_CLIP",       "CLIP / Vacant Lot / Blight",
  "L_INITIAL",    "Lead Inspection",
  "L_FINAL",      "Lead Inspection",
  "L_ENCAPS_I",   "Lead Abatement / Encapsulation",
  "L_BARGEBRD",   "Lead / Exterior Abatement",
  "L_STUCCO",     "Lead / Exterior Abatement",
  "L_LATERAL",    "Lead / Exterior Abatement",
  "L_COMPLY",     "Lead Compliance",
  "L_REINSPCT",   "Lead Reinspection",
  "L_ADJACENT",   "Lead / Adjacent Property",
  "L_SIDEWALK",   "Lead / Exterior Surface",
  "L_FOOTWAY",    "Lead / Exterior Surface",
  "L_WATERPRF",   "Lead / Exterior Surface",
  "L_CLRFLOOR",   "Lead / Clearance",
  "L_WOODCHK",    "Lead / Clearance",
  "L_MAINT",      "Lead / Maintenance",
  "L_STARTWRK",   "Lead Abatement / Work Start",
  "L_TESTDIG",    "Lead / Soil or Test Dig",
  "L_MEASURE",    "Lead / Measurement",
  "VAC INSP",     "Vacant Property",
  "VACLOT",       "Vacant Lot",
  "BC INSP",      "Building/Code Inspection",
  "BRU INSP",     "Building/Code Inspection",
  "HC FAMILY",    "Housing / Residential Program",
  "HC HI RISE",   "Housing / High-Rise",
  "HC PCH INS",   "Housing Program Inspection",
  "HC MENTOR",    "Housing Program / Administrative",
  "HC ZBA",       "Zoning / Appeals (Housing)",
  "HC SHELTER",   "Housing / Shelter",
  "HC BOARD",     "Housing / Board",
  "HC CLA",       "Housing / Administrative",
  "WM STATION",   "Weights & Measures",
  "WM SMSCALE",   "Weights & Measures",
  "WM SCANNER",   "Weights & Measures",
  "WM DEVICES",   "Weights & Measures",
  "WM METER",     "Weights & Measures"
)

clean_investigation_type <- function(x, lookup = investigationtype_lookup) {
  key_dt <- as.data.table(lookup)[, .(key = toupper(trimws(raw)), clean)]
  inp <- data.table(i = seq_along(x), raw = x)[, key := toupper(trimws(raw))]
  res <- key_dt[inp, on = .(key), nomatch = NA][order(i)][, ifelse(is.na(clean), raw, clean)]
  return(res)
}

severe_investigation_types <- c(
  "Code Enforcement (Housing)",
  "Code Enforcement (Pre-Court)",
  "Hazardous Material",
  "Lead Inspection",
  "Lead Abatement / Encapsulation",
  "Lead Abatement / Work Start",
  "Lead / Exterior Abatement",
  "Lead Compliance",
  "Lead Reinspection"
)

# ---------- Pre-process: add parcel_number and filter to rentals ----------
permits[, parcel_number := opa_account_num]
complaints[, parcel_number := opa_account_num]
violations[, parcel_number := opa_account_num]
investigations[, parcel_number := opa_account_num]

permits        <- permits[parcel_number %in% rental_pids$parcel_number]
complaints     <- complaints[parcel_number %in% rental_pids$parcel_number]
violations     <- violations[parcel_number %in% rental_pids$parcel_number]
investigations <- investigations[parcel_number %in% rental_pids$parcel_number]

logf("After filtering to rental parcels:", log_file = log_file)
logf("  Permits: ", nrow(permits), " rows", log_file = log_file)
logf("  Complaints: ", nrow(complaints), " rows", log_file = log_file)
logf("  Violations: ", nrow(violations), " rows", log_file = log_file)
logf("  Investigations: ", nrow(investigations), " rows", log_file = log_file)

# ---------- Standardize types (once, reused across agg levels) ----------
permits[, permit_standardized := clean_permit_type(permittype)]
valid_permits <- permits[, .N, by = permit_standardized][order(-N)][N > 10000, permit_standardized]

complaints[, complaint_type_standardized := clean_complaint_type(complaintcodename)]
valid_complaints <- complaints[, .N, by = complaint_type_standardized][order(-N)][N > 10000, complaint_type_standardized]

violations[, violation_standardized := fifelse(
  caseprioritydesc == "", "OTHER", toupper(trimws(caseprioritydesc))
)]
valid_violations <- violations[, .N, by = violation_standardized][order(-N)][N > 10000, violation_standardized]

investigations[, investigation_standardized := clean_investigation_type(investigationtype)]
valid_investigations <- investigations[, .N, by = investigation_standardized][order(-N)][N > 10000, investigation_standardized]

# ========== Build panels for each aggregation level ==========
for (agg_level in c("year", "quarter", "month")) {

  logf("--- Building ", agg_level, "-level panel ---", log_file = log_file)

  # Add period column for this agg level
  permits[, period := to_period(permitissuedate, agg_level)]
  complaints[, period := to_period(complaintdate, agg_level)]
  violations[, period := to_period(violationdate, agg_level)]
  investigations[, period := to_period(investigationcompleted, agg_level)]

  # --- Permits ---
  permits_agg_long <- permits[
    permit_standardized %in% valid_permits,
    .(num_permits = .N),
    by = .(parcel_number, period, permit_standardized)
  ]
  permits_agg_long[, total_permits := sum(num_permits), by = .(parcel_number, period)]

  permits_agg <- pivot_wider(
    permits_agg_long,
    names_from  = permit_standardized,
    values_from = num_permits,
    values_fill = 0
  ) |> janitor::clean_names() |> as.data.table()

  # --- Complaints ---
  complaints_agg_long <- complaints[
    complaint_type_standardized %in% valid_complaints,
    .(num_complaints = .N),
    by = .(parcel_number, period, complaint_type_standardized)
  ]
  # Totals are period-specific counts by (parcel_number, period), not cumulative across time.
  complaints_agg_long[, severe_flag := complaint_type_standardized %chin% severe_complaint_types]
  complaints_agg_long[, `:=`(
    total_complaints = sum(num_complaints),
    total_severe_complaints = sum(num_complaints[severe_flag], na.rm = TRUE)
  ), by = .(parcel_number, period)]
  complaints_agg_long[, severe_flag := NULL]

  complaints_agg <- pivot_wider(
    complaints_agg_long,
    names_from  = complaint_type_standardized,
    values_from = num_complaints,
    values_fill = 0
  ) |> janitor::clean_names() |> as.data.table()

  # --- Violations ---
  violations_agg_long <- violations[
    violation_standardized %in% valid_violations,
    .(num_violations = .N),
    by = .(parcel_number, period, violation_standardized)
  ]
  violations_agg_long[, severe_flag := violation_standardized %chin% severe_violation_types]
  violations_agg_long[, `:=`(
    total_violations        = sum(num_violations),
    total_severe_violations = sum(num_violations[severe_flag], na.rm = TRUE)
  ), by = .(parcel_number, period)]
  violations_agg_long[, severe_flag := NULL]

  violations_agg <- pivot_wider(
    violations_agg_long,
    names_from  = violation_standardized,
    values_from = num_violations,
    values_fill = 0
  ) |> janitor::clean_names() |> as.data.table()

  # --- Investigations ---
  investigations_agg_long <- investigations[
    investigation_standardized %in% valid_investigations,
    .(num_investigations = .N),
    by = .(parcel_number, period, investigation_standardized)
  ]
  investigations_agg_long[, severe_flag := investigation_standardized %chin% severe_investigation_types]
  investigations_agg_long[, `:=`(
    total_investigations        = sum(num_investigations),
    total_severe_investigations = sum(num_investigations[severe_flag], na.rm = TRUE)
  ), by = .(parcel_number, period)]
  investigations_agg_long[, severe_flag := NULL]

  investigations_agg <- pivot_wider(
    investigations_agg_long,
    names_from  = investigation_standardized,
    values_from = num_investigations,
    values_fill = 0
  ) |> janitor::clean_names() |> as.data.table()

  # --- Build parcel × period grid (rental parcels only) ---
  all_periods <- period_seq(domain_start, domain_end, agg_level)
  parcel_grid <- CJ(parcel_number = rental_pids$parcel_number, period = all_periods)

  # --- Suffix & merge ---
  permits_agg <- permits_agg |>
    rename_with(~paste0(.x, "_permit_count"), -c(parcel_number, period, total_permits))

  complaints_agg <- complaints_agg |>
    rename_with(~paste0(.x, "_complaint_count"), -c(parcel_number, period, total_complaints, total_severe_complaints))

  violations_agg <- violations_agg |>
    rename_with(~paste0(.x, "_violation_count"), -c(parcel_number, period, total_violations, total_severe_violations))

  investigations_agg <- investigations_agg |>
    rename_with(~paste0(.x, "_investigation_count"), -c(parcel_number, period, total_investigations, total_severe_investigations))

  building_data <- parcel_grid |>
    merge(permits_agg,        by = c("parcel_number","period"), all.x = TRUE) |>
    merge(complaints_agg,     by = c("parcel_number","period"), all.x = TRUE) |>
    merge(violations_agg,     by = c("parcel_number","period"), all.x = TRUE) |>
    merge(investigations_agg, by = c("parcel_number","period"), all.x = TRUE)

  # Fill NAs with zeros for count columns
  count_cols <- setdiff(names(building_data), c("parcel_number", "period"))
  for (col in count_cols) {
    set(building_data, which(is.na(building_data[[col]])), col, 0L)
  }

  # Derive year
  building_data[, year := as.integer(substr(period, 1, 4))]

  if (building_data[, any(total_severe_complaints > total_complaints, na.rm = TRUE)]) {
    stop("Assertion failed: total_severe_complaints cannot exceed total_complaints within a period.")
  }
  if (building_data[, any(total_severe_violations > total_violations, na.rm = TRUE)]) {
    stop("Assertion failed: total_severe_violations cannot exceed total_violations within a period.")
  }
  if (building_data[, any(total_severe_investigations > total_investigations, na.rm = TRUE)]) {
    stop("Assertion failed: total_severe_investigations cannot exceed total_investigations within a period.")
  }

  # --- Assertions ---
  n_total <- nrow(building_data)
  n_unique_keys <- building_data[, uniqueN(paste(parcel_number, period))]
  if (n_total != n_unique_keys) {
    logf("WARNING: Duplicate keys in ", agg_level, " panel!", log_file = log_file)
    stop("Assertion failed: (parcel_number, period) must be unique at ", agg_level, " level")
  }

  # --- Write ---
  outfile <- p_proc(cfg, glue("panels/building_data_rental_{agg_level}.parquet"))
  write_parquet(building_data, outfile)

  logf("  Wrote ", format(nrow(building_data), big.mark = ","), " rows (",
       uniqueN(building_data$parcel_number), " parcels × ",
       length(all_periods), " periods) to ", outfile, log_file = log_file)
}

logf("=== Finished make-rental-building-data.R ===", log_file = log_file)
