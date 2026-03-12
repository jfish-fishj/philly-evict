## ============================================================
## build-owner-linkage.R
## ============================================================
## Purpose: Standardize RTT grantee names, exact-match to create
##          owner_group_id, build portfolio sizes, write QA outputs.
##
## Inputs (from config):
##   - rtt_clean (from process-rtt-data.R)
##
## Outputs:
##   - data/processed/xwalks/owner_linkage_xwalk.csv
##       Primary key: (grantee_upper, PID). Corps have PID=NA; persons have actual PID.
##   - output/qa/owner_linkage_top50.csv
##   - output/qa/owner_linkage_random_sample.csv
##   - output/qa/owner_linkage_qa.txt
##   - output/logs/build-owner-linkage.log
##
## NOTE: owner_portfolio (keyed on conglomerate_id) is now written by
##       build-ownership-panel.R after Phase 2+3 entity reconciliation.
##
## Phase 1: Exact match on standardized names only.
## Phase 2 (deferred): Fuzzy matching, mailing address linkage.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
})

source("r/config.R")
source("r/helper-functions.R")

cfg <- read_config()

# ============================================================
# Financial intermediary detection
# ============================================================
# Banks, GSEs, and mortgage servicers hold properties during foreclosure limbo
# but are NOT real landlords. Flagging them prevents their mailing addresses
# from acting as hub nodes in the Phase 2 entity-linkage graph.
# Applied to owner_std (post-standardization: suffixes like BANK/CORP stripped),
# so "WELLS FARGO BANK NA" -> "WELLS FARGO" is caught by the exact name match.
.fi_words <- c(
  # GSEs
  "FANNIE MAE", "FEDERAL NATIONAL MORTGAGE",
  "FREDDIE MAC", "FEDERAL HOME LOAN", "FEDERAL HOME LOAN MORTGAGE",
  # Major banks (post-suffix-stripping these reduce to core names)
  "JP MORGAN", "JPMORGAN", "CHASE",
  "WELLS FARGO", "BANK OF AMERICA", "CITIBANK", "CITIGROUP",
  "PNC", "TD BANK", "CITIZENS BANK", "COMMERCE BANK",
  # Servicers
  "NATIONSTAR", "OCWEN", "SELECT PORTFOLIO", "CARRINGTON",
  "PHH", "DITECH", "LAKEVIEW LOAN", "BAYVIEW LOAN",
  # Government / HUD
  "DEPARTMENT OF HOUSING", "DEPT OF HOUSING",
  "HUD", "SECRETARY OF HOUSING", "SECRETARY OF VETERANS"
)
.fi_generic <- c(
  "\\bBANK\\b", "\\bBANKS\\b",
  "\\bMORTGAGE ASSOC", "\\bMORTGAGE CORP\\b",
  "\\bFEDERAL SAVINGS\\b", "\\bSAVINGS BANK\\b",
  "\\bSAVINGS ASSOC\\b", "\\bNATIONAL ASSOC\\b"
)
financial_intermediary_regex <- regex(
  paste(c(
    paste0("(", paste(.fi_words,   collapse = "|"), ")"),
    paste(.fi_generic, collapse = "|")
  ), collapse = "|"),
  ignore_case = TRUE
)

# ============================================================
# Owner category regexes (applied to owner_std, post-standardization)
# Priority order (highest first):
#   Financial-Intermediary > Government-Federal > Government-Local >
#   PHA > Nonprofit > Religious > Trust > For-profit corp > Person
# ============================================================

# PHA: Philadelphia Housing Authority and its affiliates / subsidiaries.
# Tolerates abbreviation (PHILA) and omission of AUTHORITY.
pha_owner_regex <- regex(paste(c(
  "PHILADELPHIA\\s+HOUSING",
  "PHILA[.\\s]?HOUS[A-Z\\s.]{0,20}AUTH",
  "\\bPAPMC\\b",
  "AFFILIATE\\s+OF\\s+THE\\s+PHILA"
), collapse = "|"), ignore_case = TRUE)

# Federal government entities (HUD, VA, SBA, IRS, FDIC, etc.)
federal_gov_regex <- regex(paste(c(
  "\\bHUD\\b",
  "DEPT?\\.?\\s+OF\\s+HOU[A-Z]{0,5}",        # HUD long form
  "SECRETARY\\s+OF\\s+HOU[A-Z]{0,5}",
  "SECRETARY\\s+OF\\s+VET[A-Z]{0,4}",
  "VETERANS?\\s+AFFAIR[S]?",
  "SMALL\\s+BUSINESS\\s+ADM[A-Z]{0,3}",
  "\\bSBA\\b",
  "INTERNAL\\s+REVENUE",
  "\\bFDIC\\b",
  "FEDERAL\\s+DEPOSIT\\s+INS[A-Z]{0,6}",
  "RESOLUTION\\s+TRUST",
  "GOVERNMENT\\s+NATIONAL\\s+MORTGAGE",
  "\\bGNMA\\b",
  "UNITED\\s+STATES\\s+(GOVERNMENT|DEPT|DEPARTMENT|OF\\s+AMER)",
  "U\\.?S\\.?\\s+GOVERNMENT"
), collapse = "|"), ignore_case = TRUE)

# Local/state government (Pennsylvania and Philadelphia-specific).
# Uses PHILA[A-Z]{0,7} to tolerate PHILA/PHILAD/PHILADELPH/PHILADELPHIA variants.
local_gov_regex <- regex(paste(c(
  "CITY\\s+OF\\s+PHILA[A-Z]{0,7}",
  "PHILA[A-Z]{0,7}\\s+CITY\\b",
  "MUNICIPALITY\\s+OF\\s+PHILA[A-Z]{0,7}",
  "COMMONWEALTH\\s+OF\\s+P[AE][A-Z]{0,10}",
  "STATE\\s+OF\\s+P[AE][A-Z]{0,10}",
  "REDEVELOPMENT\\s+AUTH[A-Z]{0,4}",       # PRA/RDA
  "\\bR\\.?D\\.?A\\.?\\b",
  "\\bP\\.?R\\.?A\\.?\\b(?!\\s+[A-Z]{4})", # PRA but not PRACTICE/PRAYER etc.
  "PORT\\s+AUTH[A-Z]{0,5}",
  "DELAWARE\\s+RIVER\\s+PORT",
  "SCHOOL\\s+DIST[A-Z]{0,5}",
  "BOARD\\s+OF\\s+EDUC[A-Z]{0,5}",
  "\\bSEPTA\\b",
  "SOUTHEASTERN\\s+PA[A-Z]{0,7}\\s+TRANSP[A-Z]{0,4}",
  "WATER\\s+REV[A-Z]{0,5}",                # Water Revenue Bureau
  "PHILA[A-Z]{0,7}\\s+WATER",
  "\\bPENN\\s*DOT\\b|\\bPENNDOT\\b",
  "TRANSIT\\s+AUTH[A-Z]{0,4}",
  "PARKING\\s+AUTH[A-Z]{0,4}",
  "FAIRMOUNT\\s+PARK",
  "PHILA[A-Z]{0,7}\\s+PARK[A-Z]{0,3}\\s+AUTH",
  "INDUSTRIAL\\s+DEV[A-Z]{0,4}",           # PIDC
  "\\bPIDC\\b",
  "COUNTY\\s+OF\\s+PHILA[A-Z]{0,7}",
  "PHILA[A-Z]{0,7}\\s+COUNTY",
  "HOUSING\\s+FINANC[A-Z]{0,5}\\s+AGENC[A-Z]{0,2}", # PHFA
  "\\bPHFA\\b",
  "PHILA[A-Z]{0,7}\\s+AUTH[A-Z]{0,5}",    # catch-all for unnamed Philly authorities
  "\\bAMTRAK\\b"
), collapse = "|"), ignore_case = TRUE)

# Nonprofits: CDCs, community land trusts, affordable housing orgs, major charities,
# and educational nonprofits (universities with known nonprofit status).
nonprofit_regex <- regex(paste(c(
  "\\bNON[\\s\\-]?PROFIT\\b",
  "\\bCHARITABL[EY]\\b",
  "COMMUNITY\\s+DEV[A-Z]{0,6}\\s+(CORP[A-Z]{0,4}|ORG[A-Z]{0,4}|ASSOC[A-Z]{0,4})",
  "\\bCDC\\b",
  "COMMUNITY\\s+LAND\\s+TRUST",
  "\\bCLT\\b",
  "AFFORDABLE\\s+HOUS[A-Z]{0,4}",
  "WORKFORCE\\s+HOM[A-Z]{0,2}",
  "NEIGHBORHOOD\\s+(REST[A-Z]{0,8}|PRES[A-Z]{0,10}|SERV[A-Z]{0,5})",
  "\\bHABITAT\\s+FOR\\s+HUMAN[A-Z]{0,4}\\b",
  "\\bUNITED\\s+WAY\\b",
  "\\bSALVATION\\s+ARMY\\b",
  "\\bRED\\s+CROSS\\b",
  "\\bY[WM]CA\\b",
  "PEOPLES?\\s+EMERGENCY",
  "\\bIMPACT\\s+SERVICE[S]?\\b",
  "HOUSING\\s+COUNSEL[A-Z]{0,4}",
  "COMMUNITY\\s+FOUND[A-Z]{0,5}",
  "HOUSING\\s+FOUND[A-Z]{0,5}",
  # Affordable / supportive housing structure patterns (generalizable).
  # NOTE: patterns avoid requiring CORP/INC suffixes since standardize_corp_name() strips them.
  "HOUSING\\s+DEV[A-Z]{0,8}",                  # Housing Development (Corp/Inc, abbrev or full)
  "\\bHDC\\b",                                  # Housing Development Corporation abbrev
  "SECTION\\s+811",                             # Federal 811 supportive housing (nonprofit-only)
  "TRANSITIONAL\\s+HOUS[A-Z]{0,4}",            # Transitional housing (overwhelmingly nonprofit)
  "\\bINTERFAITH\\b",                           # Interfaith housing organizations
  "ELDER\\s+SERVICE[S]?",                       # Elder services organizations
  "REVITALI[ZS][A-Z]{0,5}",                    # Revitalization orgs (CORP stripped; enough signal alone)
  # Named Philly nonprofits identified from NHPD active-property roster
  "\\bPROJECT\\s+HOME\\b",
  "\\bHELP\\s+USA\\b",
  "MERCY.{0,3}DOUGLASS",                        # Mercy-Douglass Human Services (multiple entities)
  "RESOURCES\\s+FOR\\s+HUMAN\\s+DEV[A-Z]{0,8}",  # Resources for Human Development
  "\\bPRESBY\\s+INSPIRED",                      # Presby Inspired Life (fmr. Presbyterian Medical)
  "\\bFRIENDS\\s+REHABIL[A-Z]{0,8}\\s+PROGRAM\\b",  # Friends Rehabilitation Program (ITATION=7 chars)
  # Known Philly nonprofits from np_regex in analyze-filing-decomposition.R
  "\\bOCTAVIA\\s+HILL",
  "\\bSARAH\\s+ALLEN\\s+COMMUNITY",
  "\\bMLK\\s+AFFORD",
  "\\bNEW\\s+LIFE\\s+AFFORD",
  "\\bRENAISSANCE\\s+COMMUNITY\\s+DEV",
  "\\bHELP\\s+PA\\s+AFFORD",
  # Educational nonprofits — checked before Religious so TEMPLE UNIV doesn't hit Religious
  "UNIVERSITY\\s+OF\\s+PENN[A-Z]{0,10}",
  "\\bUPENN\\b",
  "DREXEL\\s+UNIV[A-Z]{0,7}",
  "TEMPLE\\s+UNIV[A-Z]{0,7}",
  "JEFFERSON\\s+(UNIV[A-Z]{0,7}|HOSP[A-Z]{0,5}|HEALTH)",
  "PENN\\s+MEDICINE",
  "CHILDREN[']?S\\s+HOSP[A-Z]{0,5}"
), collapse = "|"), ignore_case = TRUE)

# Religious organizations: denominations, congregations, parishes, etc.
# Common abbreviations: CONG., PRESB., METH., BAPT., EVAN., A.M.E.
religious_regex <- regex(paste(c(
  "\\bCHURCH[ES]?\\b",
  "\\bPARISH[ES]?\\b",
  "\\bARCHDIOCES[EI][S]?\\b",
  "\\bDIOCES[EI][S]?\\b",
  "\\bCATHEDRAL[S]?\\b",
  "\\bCONGREGATION[S]?\\b",
  "\\bCONG\\.\\b",                          # abbrev
  "\\bSYNAGOGU[ES]?\\b",
  "\\bSYNAGOG\\b",                          # truncation
  "\\bMOSQU[EY][S]?\\b",
  "\\bMINISTR[YI][ES]?\\b|\\bMINISTR\\.\\b",
  "\\bBAPTIST\\b|\\bBAPT\\.\\b",
  "\\bMETHODIST\\b|\\bMETH\\.\\b",
  "\\bPRESBYTERIAN[S]?\\b|\\bPRESBYTER[Y]?\\b|\\bPRESB\\.\\b",
  "\\bLUTHERAN[S]?\\b",
  "\\bCATHOLIC\\b",
  "ROMAN\\s+CATH[A-Z]{0,4}",
  "\\bEPISCOPAL[A-Z]{0,3}\\b",
  "\\bPENTECOSTAL\\b",
  "\\bEVANGELICAL\\b|\\bEVANGEL\\.?\\b",
  "\\bADVENTIST\\b",
  "\\bQUAKER[S]?\\b",
  "\\bSOCIETY\\s+OF\\s+FRIEND[S]?\\b",     # Quakers formal name
  "\\bAME\\b",                              # African Methodist Episcopal
  "AFRICAN\\s+METH[A-Z]{0,5}",
  "\\bGOSPEL\\b",
  "\\bBIBLE\\b",
  "\\bZION[A-Z]{0,3}\\b",                  # many Zion churches in Philly
  "JEHOVAH[']?S\\s+WITNESS[ES]?",
  "LATTER[\\s\\-]DAY\\s+SAINT[S]?|\\bLDS\\b",
  "MUSLIM[A-Z]{0,4}|ISLAM[A-Z]{0,4}",
  "JEWISH\\s+(FED[A-Z]{0,6}|COMM[A-Z]{0,6}|CENTER|FAMILY)",
  "\\bTEMPLE\\b(?!\\s+UNIV)"               # Temple-as-religious-institution (not Temple Univ)
), collapse = "|"), ignore_case = TRUE)

# assign_owner_category: priority-ordered classification using fcase().
# Applied to owner_std (post-standardization), so entity suffixes are already stripped.
assign_owner_category <- function(owner_std, is_corp, is_financial_intermediary, is_trust) {
  nm <- coalesce(owner_std, "")
  fcase(
    is_financial_intermediary == TRUE,           "Financial-Intermediary",
    str_detect(nm, federal_gov_regex),           "Government-Federal",
    str_detect(nm, local_gov_regex),             "Government-Local",
    str_detect(nm, pha_owner_regex),             "PHA",
    str_detect(nm, nonprofit_regex),             "Nonprofit",
    str_detect(nm, religious_regex),             "Religious",
    is_trust == TRUE,                            "Trust",
    is_corp == TRUE,                             "For-profit corp",
    default =                                    "Person"
  )
}

log_file <- p_out(cfg, "logs", "build-owner-linkage.log")

# Ensure output dirs exist
qa_dir <- p_out(cfg, "qa")
fs::dir_create(qa_dir, recurse = TRUE)
fs::dir_create(dirname(p_product(cfg, "owner_linkage_xwalk")), recurse = TRUE)

logf("=== Starting build-owner-linkage.R ===", log_file = log_file)

# ============================================================
# SECTION 0: Load data
# ============================================================
logf("--- SECTION 0: Loading RTT data ---", log_file = log_file)

normalize_pid <- function(x) {
  stringr::str_pad(as.character(x), width = 9, side = "left", pad = "0")
}

rtt <- fread(p_product(cfg, "rtt_clean"))
rtt[, PID := normalize_pid(PID)]
rtt[, year := as.integer(year)]
rtt[, is_sheriff_deed := grepl("SHERIFF", toupper(document_type))]
logf("  rtt_clean: ", nrow(rtt), " rows, ", rtt[, uniqueN(PID)], " unique PIDs",
     log_file = log_file)
logf("  Sheriff's deeds: ", rtt[is_sheriff_deed == TRUE, .N], " (",
     round(100 * rtt[is_sheriff_deed == TRUE, .N] / nrow(rtt), 1), "%)",
     log_file = log_file)

# ============================================================
# SECTION 1: Name standardization
# ============================================================
logf("--- SECTION 1: Name standardization ---", log_file = log_file)

# Extract raw grantee names
rtt[, grantee_upper := toupper(trimws(as.character(grantees)))]
n_grantees <- rtt[, uniqueN(grantee_upper)]
logf("  Unique raw grantee names: ", n_grantees, log_file = log_file)

# Classify corporate vs individual using existing business_regex from helper-functions.R
rtt[, is_corp := str_detect(grantee_upper, business_regex)]
logf("  Corporate grantees: ", rtt[is_corp == TRUE, uniqueN(grantee_upper)],
     " (", round(100 * rtt[is_corp == TRUE, uniqueN(grantee_upper)] / n_grantees, 1), "%)",
     log_file = log_file)

# --- Financial intermediary classification ---
# Applied after owner_std is computed (below), but we pre-classify on grantee_upper
# here to ensure it's available for the standardize step QA.
# Final classification on owner_std happens after standardization.

# --- Business name standardization ---
# Only strip legal entity-type suffixes, NOT descriptive business words.
# Stripping words like PROPERTIES/REALTY/DEVELOPMENT caused over-merging
# (e.g., "PHILLY DEVELOPMENT LLC" and "PHILLY INVESTMENTS LLC" both -> "PHILLY").
entity_suffixes <- c(
  "LLC", "L\\.L\\.C", "L L C",
  "INC", "INCORPORATED",
  "CORP", "CORPORATION",
  "LP", "L\\.P\\.", "L P",
  "LLP",
  "LTD", "LIMITED",
  "CO",
  "COMPANY"
)

# Build a single regex for business suffix removal (word-boundary anchored)
suffix_pattern <- paste0("\\b(", paste(entity_suffixes, collapse = "|"), ")\\b")

# Phase 1: Only standardize+group corporate names. Individual person names
# are NOT grouped across transfers because "SMITH ROBERT" buying two different
# buildings could easily be two different people. Reliable person linkage
# requires address matching (Phase 2). For 10+ unit buildings, 97% of
# top-evicting transfers are corporate, so this covers the main use case.
standardize_corp_name <- function(name) {
  x <- name

  # Step 1: Remove business suffixes
  x <- str_remove_all(x, regex(suffix_pattern, ignore_case = TRUE))

  # Step 1b: Bank name normalization (applied after suffix removal so "BANK NA" → "BANK")
  # "U S BANK" → "US BANK"; "WELLS FARGO FA" → "WELLS FARGO"; etc.
  x <- str_replace_all(x, "\\bU\\s+S\\b", "US")     # spaced abbreviation
  x <- str_replace(x, "\\bTRUS\\b\\s*$", "TRUST")   # normalize TRUS → TRUST (check before TR)
  x <- str_replace(x, "\\bTR\\b\\s*$",   "TRUST")   # normalize TR   → TRUST
  x <- str_remove(x, "\\bFA\\b\\s*$")                # federal association
  x <- str_remove(x, "\\bN\\s+A\\b\\s*$")            # national association (spaced)
  x <- str_squish(x)

  # Step 2: Remove trailing punctuation (periods, commas, dashes)
  x <- str_remove_all(x, "[.,;:\\-]+$")

  # Step 3: Remove leading "THE "
  x <- str_remove(x, "^THE\\s+")

  # Step 4: Strip trailing Roman numerals (LLC series identifiers)
  x <- str_remove(x, "\\s+(VI{0,3}|IV|IX|XI{0,3}|III|II|I|V)\\s*$")

  # Step 5: Collapse whitespace
  x <- str_squish(x)

  # Step 6: Remove trailing numbers (e.g., "SOME LLC 2" after LLC stripped)
  x <- str_remove(x, "\\s+\\d+$")

  x <- str_squish(x)
  x
}

# Corporate names: standardize to enable grouping across LLC variants
# Individual names: keep raw grantee_upper (each is its own group)
rtt[is_corp == TRUE, owner_std := standardize_corp_name(grantee_upper)]
rtt[is_corp == FALSE, owner_std := grantee_upper]

# Handle empty results from standardization
rtt[owner_std == "" | is.na(owner_std), owner_std := grantee_upper]

# Classify trusts: names containing TRUST/TRUSTS (post-standardization, so TR → TRUST already done).
# Trusts are NOT treated as corporate entities for grouping purposes — they use
# person-style (name+PID) grouping to prevent over-merging via mailing addresses.
.trust_regex <- regex("\\bTRUST[S]?\\b", ignore_case = TRUE)
rtt[, is_trust := str_detect(coalesce(owner_std, grantee_upper), .trust_regex)]

# Classify financial intermediaries on owner_std (post-standardization).
# Suffixes like BANK NA / CORP / NA have been stripped, so "WELLS FARGO BANK NA"
# is now "WELLS FARGO" — matched by the exact word list.
rtt[, is_financial_intermediary := str_detect(
  coalesce(owner_std, grantee_upper),
  financial_intermediary_regex
)]

rtt[, owner_category := assign_owner_category(owner_std, is_corp, is_financial_intermediary, is_trust)]

n_fi_entities <- rtt[is_financial_intermediary == TRUE, uniqueN(owner_std)]
logf("  Financial intermediary grantees: ", rtt[is_financial_intermediary == TRUE, .N],
     " transfers, ", n_fi_entities, " unique std names",
     log_file = log_file)
fi_top10 <- rtt[is_financial_intermediary == TRUE,
  .(n_transfers = .N, n_pids = uniqueN(PID)), by = owner_std
][order(-n_transfers)][1:min(.N, 15L)]
logf("  Top financial intermediaries:", log_file = log_file)
for (i in seq_len(nrow(fi_top10))) {
  logf("    ", fi_top10$owner_std[i], ": ", fi_top10$n_transfers[i],
       " transfers, ", fi_top10$n_pids[i], " PIDs", log_file = log_file)
}

n_std <- rtt[, uniqueN(owner_std)]
logf("  Unique standardized names: ", n_std, " (from ", n_grantees, " raw names)",
     log_file = log_file)
logf("  Compression ratio: ", round(n_grantees / n_std, 2), "x", log_file = log_file)

cat_dist <- rtt[, .(n_transfers = .N, n_std_names = uniqueN(owner_std)), by = owner_category]
setorder(cat_dist, -n_transfers)
logf("  owner_category distribution (transfers | unique std names):", log_file = log_file)
for (i in seq_len(nrow(cat_dist))) {
  logf("    ", cat_dist$owner_category[i], ": ",
       cat_dist$n_transfers[i], " transfers, ",
       cat_dist$n_std_names[i], " unique names", log_file = log_file)
}

# ============================================================
# SECTION 2: Exact matching -> owner_group_id
# ============================================================
logf("--- SECTION 2: Building owner groups ---", log_file = log_file)

# Corps + FIs (non-trust): one entity per standardized owner_std (groups LLC variants of same name).
# Trusts are EXCLUDED from corp-style grouping — they use person-style (name+PID) below.
# This prevents "SMITH FAMILY TRUST" on different parcels from being merged into a single
# entity that could then act as a hub in the Phase 2 mailing-address graph.
corp_groups <- unique(
  rtt[(is_corp == TRUE & is_trust == FALSE) | is_financial_intermediary == TRUE, .(owner_std)]
)[, owner_group_id := .I]
rtt[(is_corp == TRUE & is_trust == FALSE) | is_financial_intermediary == TRUE,
    owner_group_id := corp_groups[.SD, on = "owner_std", owner_group_id]]

# Persons + Trusts (non-FI): one entity per (grantee_upper, PID).
# IDs start after max corp ID to keep the two namespaces separate.
max_corp_id <- if (nrow(corp_groups) > 0L) max(corp_groups$owner_group_id) else 0L
.in_corp_group <- (rtt$is_corp == TRUE & rtt$is_trust == FALSE) |
                   rtt$is_financial_intermediary == TRUE
pers_trust_combos <- unique(
  rtt[!.in_corp_group, .(grantee_upper, PID)]
)[, owner_group_id := .I + max_corp_id]
rtt[!.in_corp_group,
    owner_group_id := pers_trust_combos[.SD, on = .(grantee_upper, PID), owner_group_id]]

n_corp_groups  <- nrow(corp_groups)
n_trust_groups <- rtt[is_trust == TRUE & is_financial_intermediary == FALSE,
                      uniqueN(owner_group_id)]
n_pers_groups  <- nrow(pers_trust_combos) - n_trust_groups
logf("  Corporate entity groups (unique owner_std): ", n_corp_groups,  log_file = log_file)
logf("  Trust entity groups (unique name+PID):      ", n_trust_groups, log_file = log_file)
logf("  Person entity groups (unique name+PID):     ", n_pers_groups,  log_file = log_file)
logf("  Total entity groups: ", n_corp_groups + nrow(pers_trust_combos), log_file = log_file)

# ============================================================
# SECTION 3: QA checks on standardization
# ============================================================
logf("--- SECTION 3: QA checks ---", log_file = log_file)

# Flag very short standardized names (< 3 chars) — may be over-merged.
# Only check strict-corp names (trusts + individuals use name+PID so short names are less risky).
short_names <- unique(rtt[is_corp == TRUE & is_trust == FALSE &
                            nchar(owner_std) < 3, .(owner_std, owner_group_id)])
if (nrow(short_names) > 0) {
  logf("  WARNING: ", nrow(short_names), " owner_std values < 3 chars: ",
       paste(head(short_names$owner_std, 10), collapse = ", "),
       log_file = log_file)
}

# Flag very generic names that might cause over-merging.
# TRUST removed from this list — trust names are now person-style (name+PID), not corp-style.
generic_names <- c("PROPERTIES", "MANAGEMENT", "INVESTMENTS",
                   "HOLDINGS", "REALTY", "ASSOCIATES", "COMPANY")
generic_hits <- unique(rtt[is_corp == TRUE & is_trust == FALSE &
                              owner_std %in% generic_names, .(owner_std, owner_group_id)])
if (nrow(generic_hits) > 0) {
  logf("  WARNING: ", nrow(generic_hits), " owner_std values are generic words: ",
       paste(generic_hits$owner_std, collapse = ", "),
       log_file = log_file)
}

# ============================================================
# SECTION 4: Build crosswalk
# ============================================================
logf("--- SECTION 4: Building crosswalk ---", log_file = log_file)

# Corps/FIs (non-trust): PID = NA — entity spans all their properties (one row per grantee_upper).
corp_xwalk <- unique(rtt[(is_corp == TRUE & is_trust == FALSE) | is_financial_intermediary == TRUE,
  .(grantee_upper, PID = NA_character_, owner_std, owner_group_id, owner_category,
    is_corp, is_financial_intermediary, is_trust = FALSE)])

# Persons + Trusts (non-FI): PID = actual parcel — entity is specific to this name+property pair.
pers_trust_xwalk <- unique(rtt[!.in_corp_group,
  .(grantee_upper, PID, owner_std, owner_group_id, owner_category,
    is_corp, is_financial_intermediary, is_trust)])

xwalk <- rbind(corp_xwalk, pers_trust_xwalk)

# Primary key: (grantee_upper, PID). Corps have PID=NA (one row per unique name).
assert_unique(xwalk, c("grantee_upper", "PID"), name = "owner_linkage_xwalk")

logf("  Crosswalk rows (unique grantee_upper x PID): ", nrow(xwalk), log_file = log_file)
logf("  Corporate entries (PID=NA):      ", xwalk[is.na(PID), .N],
     " (", round(100 * xwalk[is.na(PID), .N] / nrow(xwalk), 1), "%)",
     log_file = log_file)
logf("  Trust entries (PID!=NA, is_trust): ", xwalk[!is.na(PID) & is_trust == TRUE, .N],
     log_file = log_file)
logf("  Person entries (PID!=NA):          ", xwalk[!is.na(PID) & is_trust == FALSE, .N],
     log_file = log_file)

# ============================================================
# SECTION 5: Portfolio construction moved to build-ownership-panel.R
# (post-reconciliation, keyed on conglomerate_id rather than owner_group_id)
# ============================================================

# ============================================================
# SECTION 6: Write outputs
# ============================================================
logf("--- SECTION 6: Writing outputs ---", log_file = log_file)

# Crosswalk (primary key: grantee_upper x PID; corps have PID=NA)
fwrite(xwalk, p_product(cfg, "owner_linkage_xwalk"))
logf("  Wrote owner_linkage_xwalk.csv: ", nrow(xwalk), " rows", log_file = log_file)
# NOTE: owner_portfolio is now written by build-ownership-panel.R (post-reconciliation)

# ============================================================
# SECTION 7: QA outputs
# ============================================================
logf("--- SECTION 7: QA outputs ---", log_file = log_file)

# Lightweight per-entity transfer summary for QA.
# (Portfolio bins and reconciled stats are now in build-ownership-panel.R.)
qa_by_group <- rtt[, .(
  n_properties   = uniqueN(PID),
  n_transfers    = .N,
  owner_std      = owner_std[1L],
  is_corp        = is_corp[1L],
  is_trust       = is_trust[1L],
  is_fi          = is_financial_intermediary[1L],
  first_acq_year = min(year, na.rm = TRUE),
  last_acq_year  = max(year, na.rm = TRUE)
), by = owner_group_id]

# Top 50 entity groups by distinct PIDs, with raw name variants
top50_ids <- qa_by_group[order(-n_properties)][1:min(.N, 50L), owner_group_id]
top50 <- merge(
  qa_by_group[owner_group_id %in% top50_ids],
  xwalk[owner_group_id %in% top50_ids, .(
    raw_name_variants = paste(sort(unique(grantee_upper)), collapse = " | "),
    sample_pids       = paste(sort(unique(coalesce(PID, "(corp)"))), collapse = " | ")
  ), by = owner_group_id],
  by = "owner_group_id"
)
setorder(top50, -n_properties)
fwrite(top50, file.path(qa_dir, "owner_linkage_top50.csv"))
logf("  Wrote owner_linkage_top50.csv: ", nrow(top50), " rows", log_file = log_file)

# Random sample: 50 multi-property groups + 50 single-property groups
set.seed(cfg$run$seed %||% 123)
multi_ids  <- qa_by_group[n_properties >= 2L, owner_group_id]
single_ids <- qa_by_group[n_properties == 1L, owner_group_id]
sample_ids <- c(
  sample(multi_ids,  min(50L, length(multi_ids))),
  sample(single_ids, min(50L, length(single_ids))))
sample_dt <- merge(
  qa_by_group[owner_group_id %in% sample_ids],
  xwalk[owner_group_id %in% sample_ids, .(
    raw_name_variants = paste(sort(unique(grantee_upper)), collapse = " | "),
    sample_pids       = paste(sort(unique(coalesce(PID, "(corp)"))), collapse = " | ")
  ), by = owner_group_id],
  by = "owner_group_id"
)
sample_dt[, sample_type := fifelse(n_properties == 1L, "single", "multi")]
setorder(sample_dt, -n_properties)
fwrite(sample_dt, file.path(qa_dir, "owner_linkage_random_sample.csv"))
logf("  Wrote owner_linkage_random_sample.csv: ", nrow(sample_dt), " rows", log_file = log_file)

# QA summary text file
qa_file <- file.path(qa_dir, "owner_linkage_qa.txt")
qa_lines <- c(
  "=== Owner Linkage QA Summary ===",
  paste0("Run date: ", Sys.time()),
  "",
  paste0("Raw unique grantee names: ", n_grantees),
  paste0("Standardized unique names: ", n_std),
  paste0("Compression ratio: ", round(n_grantees / n_std, 2), "x"),
  "",
  "--- Entity group counts ---",
  paste0("Corporate entity groups (unique owner_std): ", n_corp_groups),
  paste0("Trust entity groups (unique name+PID):      ", n_trust_groups),
  paste0("Person entity groups (unique name+PID):     ", n_pers_groups),
  paste0("Total entity groups: ", n_corp_groups + n_trust_groups + n_pers_groups),
  "NOTE: Trusts (names containing TRUST/TRUSTS) use person-style grouping (name+PID),",
  "      not corp-style, to prevent mailing-address over-merging in Phase 2.",
  "",
  "NOTE: Portfolio stats (n_properties, portfolio_bin) are computed post-reconciliation",
  "      in build-ownership-panel.R, keyed on conglomerate_id.",
  "",
  "--- Short/generic name warnings ---",
  paste0("Short names (< 3 chars): ", nrow(short_names)),
  if (nrow(short_names) > 0) paste0("  ", short_names$owner_std, collapse = "\n") else "  (none)",
  paste0("Generic names: ", nrow(generic_hits)),
  if (nrow(generic_hits) > 0) paste0("  ", generic_hits$owner_std, collapse = "\n") else "  (none)",
  "",
  "--- Top 10 entity groups by distinct PIDs ---"
)

top10 <- qa_by_group[order(-n_properties)][1:min(.N, 10L)]
for (i in seq_len(nrow(top10))) {
  qa_lines <- c(qa_lines, paste0(
    "  ", top10$owner_std[i], ": ", top10$n_properties[i], " PIDs, ",
    top10$n_transfers[i], " transfers (",
    top10$first_acq_year[i], "-", top10$last_acq_year[i], ")"
  ))
}

qa_lines <- c(qa_lines,
  "",
  "--- Top financial intermediaries (excluded from entity linkage graph) ---",
  paste0("FI entity groups: ", qa_by_group[is_fi == TRUE, .N])
)
for (i in seq_len(nrow(fi_top10))) {
  qa_lines <- c(qa_lines, paste0(
    "  ", fi_top10$owner_std[i], ": ", fi_top10$n_transfers[i],
    " transfers, ", fi_top10$n_pids[i], " PIDs"
  ))
}

writeLines(qa_lines, qa_file)
logf("  Wrote owner_linkage_qa.txt", log_file = log_file)

logf("=== build-owner-linkage.R complete ===", log_file = log_file)
