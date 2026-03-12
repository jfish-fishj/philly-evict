## ============================================================
## build-ownership-panel.R
## ============================================================
## Purpose: Phase 2+3 iterative entity linkage + PID x year ownership
##          panel with time-varying portfolio variables at entity and
##          conglomerate levels.
##
## Phase 2: OPA mailing-address name+address matching (registered-agent filter).
##          Corps match on (business_short_name, mailing_norm); persons/trusts
##          match on (entity_name, mailing_norm). igraph finds connected components.
## Phase 3 (new): RTT-OPA transitive linkage — if OPA 2024 says entity B
##          owns PID Y and RTT says entity C last acquired PID Y, link B and C.
## Fixed-point: repeat Phases 2+3 (up to entity_linkage_max_rounds) until
##          no new edges appear; mailing-address profile expands each round.
##
## Financial intermediaries (banks, GSEs, servicers) are EXCLUDED from all
## graph edges to prevent false hub-merging.
##
## Consumes Phase 1 outputs from build-owner-linkage.R.
##
## Inputs (from config):
##   - owner_linkage_xwalk  (Phase 1: grantee_upper x PID -> owner_group_id;
##                           corps have PID=NA, persons have actual PID)
##   - rtt_clean            (all deed transfers including sheriff's deeds)
##   - parcels_clean        (OPA 2024: owner_1, mailing_street, mailing_zip)
##
## Outputs:
##   - xwalk_entity_conglomerate  (entity -> conglomerate; link_method, consolidation_round)
##   - xwalk_pid_entity           (PID x year -> entity, is_financial_intermediary_owner)
##   - xwalk_pid_conglomerate     (PID x year -> conglomerate, is_financial_intermediary_owner)
##   - owner_portfolio            (conglomerate-level portfolio stats, portfolio_bin)
##   - name_lookup                (raw name -> entity_id + conglomerate_id)
##   - output/logs/build-ownership-panel.log
##
## Run after: build-owner-linkage.R
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(igraph)
  library(fs)
})

source("r/config.R")
source("r/helper-functions.R")

cfg    <- read_config()
log_file <- p_out(cfg, "logs", "build-ownership-panel.log")

logf("=== Starting build-ownership-panel.R ===", log_file = log_file)

# Ensure output dirs exist
fs::dir_create(dirname(p_product(cfg, "xwalk_entity_conglomerate")), recurse = TRUE)
fs::dir_create(dirname(p_product(cfg, "xwalk_pid_entity")),          recurse = TRUE)
fs::dir_create(dirname(p_product(cfg, "name_lookup")),               recurse = TRUE)

# Year range for the ownership panel
YEAR_MIN <- 2000L
YEAR_MAX <- 2023L

# Registered-agent address filter threshold (config-overridable)
# Addresses linked to > this many entities are excluded from Phase 2.
REGISTERED_AGENT_THRESHOLD <- as.integer(
  cfg$run$entity_linkage_ra_threshold %||% 300L)

# Maximum fixed-point iteration rounds (config-overridable)
MAX_ROUNDS <- as.integer(
  cfg$run$entity_linkage_max_rounds %||% 5L)

normalize_pid <- function(x) {
  stringr::str_pad(as.character(x), width = 9, side = "left", pad = "0")
}

# standardize_corp_name is defined locally in build-owner-linkage.R (not exported to
# helper-functions.R). Re-define here to keep this script self-contained.
.entity_suffixes <- c(
  "LLC", "L\\.L\\.C", "L L C",
  "INC", "INCORPORATED",
  "CORP", "CORPORATION",
  "LP", "L\\.P\\.", "L P",
  "LLP",
  "LTD", "LIMITED",
  "CO",
  "COMPANY"
)
.suffix_pattern <- paste0("\\b(", paste(.entity_suffixes, collapse = "|"), ")\\b")

standardize_corp_name <- function(name) {
  x <- name
  x <- str_remove_all(x, regex(.suffix_pattern, ignore_case = TRUE))
  # Bank/trust name normalization (kept in sync with build-owner-linkage.R Step 1b)
  x <- str_replace_all(x, "\\bU\\s+S\\b", "US")
  x <- str_replace(x, "\\bTRUS\\b\\s*$", "TRUST")   # normalize TRUS → TRUST
  x <- str_replace(x, "\\bTR\\b\\s*$",   "TRUST")   # normalize TR   → TRUST
  x <- str_remove(x, "\\bFA\\b\\s*$")
  x <- str_remove(x, "\\bN\\s+A\\b\\s*$")
  x <- str_squish(x)
  x <- str_remove_all(x, "[.,;:\\-]+$")
  x <- str_remove(x, "^THE\\s+")
  x <- str_remove(x, "\\s+(VI{0,3}|IV|IX|XI{0,3}|III|II|I|V)\\s*$")
  x <- str_squish(x)
  x <- str_remove(x, "\\s+\\d+$")
  str_squish(x)
}

# Financial intermediary regex — same definition as in build-owner-linkage.R.
# Inlined here to keep this script self-contained.
.fi_words <- c(
  "FANNIE MAE", "FEDERAL NATIONAL MORTGAGE",
  "FREDDIE MAC", "FEDERAL HOME LOAN", "FEDERAL HOME LOAN MORTGAGE",
  "JP MORGAN", "JPMORGAN", "CHASE",
  "WELLS FARGO", "BANK OF AMERICA", "CITIBANK", "CITIGROUP",
  "PNC", "TD BANK", "CITIZENS BANK", "COMMERCE BANK",
  "NATIONSTAR", "OCWEN", "SELECT PORTFOLIO", "CARRINGTON",
  "PHH", "DITECH", "LAKEVIEW LOAN", "BAYVIEW LOAN",
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

# Trust regex — same as in build-owner-linkage.R
.trust_regex <- regex("\\bTRUST[S]?\\b", ignore_case = TRUE)

# Owner category regexes — kept in sync with build-owner-linkage.R.
# Applied to owner_std (post-standardization) for OPA-only new entities.
.pha_owner_regex <- regex(paste(c(
  "PHILADELPHIA\\s+HOUSING",
  "PHILA[.\\s]?HOUS[A-Z\\s.]{0,20}AUTH",
  "\\bPAPMC\\b",
  "AFFILIATE\\s+OF\\s+THE\\s+PHILA"
), collapse = "|"), ignore_case = TRUE)

.federal_gov_regex <- regex(paste(c(
  "\\bHUD\\b",
  "DEPT?\\.?\\s+OF\\s+HOU[A-Z]{0,5}",
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

.local_gov_regex <- regex(paste(c(
  "CITY\\s+OF\\s+PHILA[A-Z]{0,7}",
  "PHILA[A-Z]{0,7}\\s+CITY\\b",
  "MUNICIPALITY\\s+OF\\s+PHILA[A-Z]{0,7}",
  "COMMONWEALTH\\s+OF\\s+P[AE][A-Z]{0,10}",
  "STATE\\s+OF\\s+P[AE][A-Z]{0,10}",
  "REDEVELOPMENT\\s+AUTH[A-Z]{0,4}",
  "\\bR\\.?D\\.?A\\.?\\b",
  "\\bP\\.?R\\.?A\\.?\\b(?!\\s+[A-Z]{4})",
  "PORT\\s+AUTH[A-Z]{0,5}",
  "DELAWARE\\s+RIVER\\s+PORT",
  "SCHOOL\\s+DIST[A-Z]{0,5}",
  "BOARD\\s+OF\\s+EDUC[A-Z]{0,5}",
  "\\bSEPTA\\b",
  "SOUTHEASTERN\\s+PA[A-Z]{0,7}\\s+TRANSP[A-Z]{0,4}",
  "WATER\\s+REV[A-Z]{0,5}",
  "PHILA[A-Z]{0,7}\\s+WATER",
  "\\bPENN\\s*DOT\\b|\\bPENNDOT\\b",
  "TRANSIT\\s+AUTH[A-Z]{0,4}",
  "PARKING\\s+AUTH[A-Z]{0,4}",
  "FAIRMOUNT\\s+PARK",
  "PHILA[A-Z]{0,7}\\s+PARK[A-Z]{0,3}\\s+AUTH",
  "INDUSTRIAL\\s+DEV[A-Z]{0,4}",
  "\\bPIDC\\b",
  "COUNTY\\s+OF\\s+PHILA[A-Z]{0,7}",
  "PHILA[A-Z]{0,7}\\s+COUNTY",
  "HOUSING\\s+FINANC[A-Z]{0,5}\\s+AGENC[A-Z]{0,2}",
  "\\bPHFA\\b",
  "PHILA[A-Z]{0,7}\\s+AUTH[A-Z]{0,5}",
  "\\bAMTRAK\\b"
), collapse = "|"), ignore_case = TRUE)

.nonprofit_regex <- regex(paste(c(
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
  "HOUSING\\s+DEV[A-Z]{0,8}",
  "\\bHDC\\b",
  "SECTION\\s+811",
  "TRANSITIONAL\\s+HOUS[A-Z]{0,4}",
  "\\bINTERFAITH\\b",
  "ELDER\\s+SERVICE[S]?",
  "REVITALI[ZS][A-Z]{0,5}",
  # Named Philly nonprofits identified from NHPD active-property roster
  "\\bPROJECT\\s+HOME\\b",
  "\\bHELP\\s+USA\\b",
  "MERCY.{0,3}DOUGLASS",
  "RESOURCES\\s+FOR\\s+HUMAN\\s+DEV[A-Z]{0,8}",
  "\\bPRESBY\\s+INSPIRED",
  "\\bFRIENDS\\s+REHABIL[A-Z]{0,8}\\s+PROGRAM\\b",
  # Known Philly nonprofits from np_regex in analyze-filing-decomposition.R
  "\\bOCTAVIA\\s+HILL",
  "\\bSARAH\\s+ALLEN\\s+COMMUNITY",
  "\\bMLK\\s+AFFORD",
  "\\bNEW\\s+LIFE\\s+AFFORD",
  "\\bRENAISSANCE\\s+COMMUNITY\\s+DEV",
  "\\bHELP\\s+PA\\s+AFFORD",
  "UNIVERSITY\\s+OF\\s+PENN[A-Z]{0,10}",
  "\\bUPENN\\b",
  "DREXEL\\s+UNIV[A-Z]{0,7}",
  "TEMPLE\\s+UNIV[A-Z]{0,7}",
  "JEFFERSON\\s+(UNIV[A-Z]{0,7}|HOSP[A-Z]{0,5}|HEALTH)",
  "PENN\\s+MEDICINE",
  "CHILDREN[']?S\\s+HOSP[A-Z]{0,5}"
), collapse = "|"), ignore_case = TRUE)

.religious_regex <- regex(paste(c(
  "\\bCHURCH[ES]?\\b",
  "\\bPARISH[ES]?\\b",
  "\\bARCHDIOCES[EI][S]?\\b",
  "\\bDIOCES[EI][S]?\\b",
  "\\bCATHEDRAL[S]?\\b",
  "\\bCONGREGATION[S]?\\b",
  "\\bCONG\\.\\b",
  "\\bSYNAGOGU[ES]?\\b",
  "\\bSYNAGOG\\b",
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
  "\\bSOCIETY\\s+OF\\s+FRIEND[S]?\\b",
  "\\bAME\\b",
  "AFRICAN\\s+METH[A-Z]{0,5}",
  "\\bGOSPEL\\b",
  "\\bBIBLE\\b",
  "\\bZION[A-Z]{0,3}\\b",
  "JEHOVAH[']?S\\s+WITNESS[ES]?",
  "LATTER[\\s\\-]DAY\\s+SAINT[S]?|\\bLDS\\b",
  "MUSLIM[A-Z]{0,4}|ISLAM[A-Z]{0,4}",
  "JEWISH\\s+(FED[A-Z]{0,6}|COMM[A-Z]{0,6}|CENTER|FAMILY)",
  "\\bTEMPLE\\b(?!\\s+UNIV)"
), collapse = "|"), ignore_case = TRUE)

assign_owner_category <- function(owner_std, is_corp, is_financial_intermediary, is_trust) {
  nm <- coalesce(owner_std, "")
  fcase(
    is_financial_intermediary == TRUE,           "Financial-Intermediary",
    str_detect(nm, .federal_gov_regex),          "Government-Federal",
    str_detect(nm, .local_gov_regex),            "Government-Local",
    str_detect(nm, .pha_owner_regex),            "PHA",
    str_detect(nm, .nonprofit_regex),            "Nonprofit",
    str_detect(nm, .religious_regex),            "Religious",
    is_trust == TRUE,                            "Trust",
    is_corp == TRUE,                             "For-profit corp",
    default =                                    "Person"
  )
}

# ============================================================
# HELPER FUNCTIONS
# ============================================================

# Business descriptor words that vary between related entities (e.g. "ABC PROPERTIES"
# vs "ABC MANAGEMENT") — stripped to produce business_short_name for corp matching.
# owner_std already has entity-type suffixes removed by standardize_corp_name().
.biz_descriptor_pattern <- paste0(
  "\\b(PROPERTIES|PROPERTY|MANAGEMENT|MGMT|INVESTMENTS|INVESTMENT|",
  "REALTY|HOLDINGS|HOLDING|DEVELOPMENT|DEVELOPERS|DEVELOPER|",
  "ASSOCIATES|ASSOC|ENTERPRISES|ENTERPRISE|VENTURES|VENTURE|",
  "GROUP|SOLUTIONS|PARTNERS|PARTNERSHIP|CAPITAL|SERVICES|SERVICE|",
  "RENTALS|RENTAL|RESOURCES|SYSTEMS|INDUSTRIES|INDUSTRY|",
  "INTERNATIONAL|INTL|BUILDERS|BUILDER|ACQUISITIONS|ACQUISITION|",
  "REAL\\s+ESTATE|REALTORS|REALTOR)\\b"
)

compute_business_short_name <- function(owner_std) {
  x <- str_remove_all(owner_std, regex(.biz_descriptor_pattern, ignore_case = TRUE))
  str_squish(x)
}

# build_name_address_edges: match entities on name + mailing address.
#
# For corporations (is_corp=TRUE, is_trust=FALSE): match on
#   (business_short_name, mailing_norm) — catches "ABC PROPERTIES" = "ABC REALTY"
#   at the same address; requires short name >= min_short_name_chars.
# For persons / trusts: match on (entity_name, mailing_norm) — exact name + address.
#
# The RA filter drops addresses shared by > ra_threshold entities (registered agents).
# No hub-entity filter needed: since corps must also share a name, a large institution
# like the City of Philadelphia cannot act as a bipartite hub even with many addresses.
#
# Returns data.table(entity_id_a, entity_id_b) — undirected entity pairs.
build_name_address_edges <- function(entity_mailing_dt, entity_attrs_dt, fi_entity_ids,
                                     ra_threshold       = REGISTERED_AGENT_THRESHOLD,
                                     min_short_name_chars = 4L,
                                     log_file = NULL) {
  # Join mailing profile with entity attributes; exclude FIs
  prof <- merge(
    unique(entity_mailing_dt[!is.na(entity_id) & !is.na(mailing_norm),
                              .(entity_id, mailing_norm)]),
    entity_attrs_dt[!(entity_id %in% fi_entity_ids),
                    .(entity_id, entity_name, business_short_name, is_corp, is_trust)],
    by = "entity_id"
  )
  if (nrow(prof) == 0L) {
    return(data.table(entity_id_a = integer(0), entity_id_b = integer(0)))
  }

  # RA filter: drop addresses shared by too many entities
  addr_counts <- prof[, .(n_entities = uniqueN(entity_id)), by = mailing_norm]
  agent_addrs <- addr_counts[n_entities > ra_threshold, mailing_norm]
  if (!is.null(log_file) && length(agent_addrs) > 0) {
    logf("    RA-filtered addresses: ", length(agent_addrs),
         " (each linked to >", ra_threshold, " entities)", log_file = log_file)
  }
  prof <- prof[!mailing_norm %in% agent_addrs]
  if (nrow(prof) == 0L) {
    return(data.table(entity_id_a = integer(0), entity_id_b = integer(0)))
  }

  # --- Corp-to-corp: same business_short_name + same mailing address ---
  corp_prof <- prof[
    is_corp == TRUE & is_trust == FALSE &
      !is.na(business_short_name) &
      nchar(trimws(business_short_name)) >= min_short_name_chars,
    .(entity_id, mailing_norm, business_short_name)
  ]
  corp_edges <- data.table(entity_id_a = integer(0), entity_id_b = integer(0))
  if (nrow(corp_prof) > 0L) {
    corp_self <- merge(corp_prof, corp_prof,
                       by = c("mailing_norm", "business_short_name"),
                       allow.cartesian = TRUE)
    corp_self <- corp_self[entity_id.x < entity_id.y,
                           .(entity_id_a = entity_id.x, entity_id_b = entity_id.y)]
    corp_edges <- unique(corp_self)
  }

  # --- Person / trust: same entity_name + same mailing address ---
  pers_prof <- prof[
    !(is_corp == TRUE & is_trust == FALSE) &
      !is.na(entity_name) & nchar(trimws(entity_name)) >= 2L,
    .(entity_id, mailing_norm, entity_name)
  ]
  pers_edges <- data.table(entity_id_a = integer(0), entity_id_b = integer(0))
  if (nrow(pers_prof) > 0L) {
    pers_self <- merge(pers_prof, pers_prof,
                       by = c("mailing_norm", "entity_name"),
                       allow.cartesian = TRUE)
    pers_self <- pers_self[entity_id.x < entity_id.y,
                           .(entity_id_a = entity_id.x, entity_id_b = entity_id.y)]
    pers_edges <- unique(pers_self)
  }

  if (!is.null(log_file)) {
    logf("    Corp-to-corp edges: ", nrow(corp_edges),
         "  Person/trust edges: ", nrow(pers_edges), log_file = log_file)
  }
  unique(rbind(corp_edges, pers_edges))
}

# build_rtt_opa_edges: for each PID where OPA owner != RTT last grantee, add an
# edge between those two entity IDs — but ONLY if the two entities are plausibly
# the same entity under different name spellings (name-similarity gate).
#
# Similarity gate:
#   Corps (is_corp=TRUE, is_trust=FALSE): require matching business_short_name
#     (≥ min_corp_short_name_chars) on both sides. Catches "ABC LLC" vs "ABC INC".
#   Persons / trusts: require ≥1 shared token of ≥ min_person_token_chars.
#     Catches "GULLE JEAN PAUL" vs "GULLE JEAN P" (share "GULLE").
#   Cross-type (corp ↔ person): no edge — prevents city/institution explosions.
#
# Without this gate, Phase 3 falsely links every entity that once received a
# property from a large government or institutional owner (City of Philadelphia,
# PHA, Redevelopment Authority), since those conveyances often appear in RTT
# but subsequent resales don't, leaving a permanent OPA≠RTT discrepancy.
build_rtt_opa_edges <- function(rtt_last_dt, opa_dt, entity_attrs_dt, fi_entity_ids,
                                min_corp_short_name_chars = 4L,
                                min_person_token_chars    = 5L,
                                log_file = NULL) {
  # rtt_last_dt: (PID, entity_id) — last RTT grantee (may be conglomerate_id)
  # opa_dt:      (PID, entity_id) — OPA 2024 owner (may be conglomerate_id)
  combined <- merge(
    rtt_last_dt[, .(PID, rtt_entity_id = entity_id)],
    opa_dt[,      .(PID, opa_entity_id = entity_id)],
    by = "PID"
  )
  combined <- combined[rtt_entity_id != opa_entity_id]
  combined <- combined[
    !(rtt_entity_id %in% fi_entity_ids) &
      !(opa_entity_id %in% fi_entity_ids)
  ]
  if (nrow(combined) == 0L) {
    return(data.table(entity_id_a = integer(0), entity_id_b = integer(0)))
  }

  # Attach name attributes for both sides
  attrs <- entity_attrs_dt[,
    .(entity_id, entity_name, business_short_name, is_corp, is_trust)
  ]
  combined <- merge(combined, attrs,
                    by.x = "rtt_entity_id", by.y = "entity_id", all.x = TRUE)
  setnames(combined,
           c("entity_name", "business_short_name", "is_corp", "is_trust"),
           c("name_rtt",    "bsn_rtt",             "is_corp_rtt", "is_trust_rtt"))
  combined <- merge(combined, attrs,
                    by.x = "opa_entity_id", by.y = "entity_id", all.x = TRUE)
  setnames(combined,
           c("entity_name", "business_short_name", "is_corp", "is_trust"),
           c("name_opa",    "bsn_opa",             "is_corp_opa", "is_trust_opa"))

  n_before <- nrow(combined)

  # Classify each side as corp (non-trust) or person/trust
  combined[, corp_rtt := is_corp_rtt == TRUE & is_trust_rtt == FALSE]
  combined[, corp_opa := is_corp_opa == TRUE & is_trust_opa == FALSE]

  # --- Corp-to-corp gate: same business_short_name (≥ min chars) ---
  corp_mask <- combined[,
    corp_rtt & corp_opa &
      !is.na(bsn_rtt) & !is.na(bsn_opa) &
      nchar(trimws(bsn_rtt)) >= min_corp_short_name_chars &
      bsn_rtt == bsn_opa
  ]

  # --- Person/trust-to-person/trust gate: ≥1 shared token of ≥ min chars ---
  pers_rows <- which(!combined$corp_rtt & !combined$corp_opa &
                       !is.na(combined$name_rtt) & !is.na(combined$name_opa))
  pers_mask <- rep(FALSE, nrow(combined))
  if (length(pers_rows) > 0L) {
    tok_rtt <- strsplit(combined$name_rtt[pers_rows], "\\s+")
    tok_opa <- strsplit(combined$name_opa[pers_rows], "\\s+")
    pers_mask[pers_rows] <- mapply(function(a, b) {
      a_long <- a[nchar(a) >= min_person_token_chars]
      b_long <- b[nchar(b) >= min_person_token_chars]
      length(a_long) > 0L && length(b_long) > 0L &&
        length(intersect(a_long, b_long)) > 0L
    }, tok_rtt, tok_opa, SIMPLIFY = TRUE)
  }

  combined <- combined[corp_mask | pers_mask]
  n_after <- nrow(combined)

  if (!is.null(log_file)) {
    logf("    Name-similarity gate: ", n_before, " candidate pairs → ",
         n_after, " passed (", n_before - n_after, " dropped)",
         log_file = log_file)
  }

  if (nrow(combined) == 0L) {
    return(data.table(entity_id_a = integer(0), entity_id_b = integer(0)))
  }
  unique(combined[, .(
    entity_id_a = pmin(rtt_entity_id, opa_entity_id),
    entity_id_b = pmax(rtt_entity_id, opa_entity_id)
  )])
}

# ============================================================
# SECTION 0: Load data
# ============================================================
logf("--- SECTION 0: Loading data ---", log_file = log_file)

xwalk     <- fread(p_product(cfg, "owner_linkage_xwalk"))
rtt       <- fread(p_product(cfg, "rtt_clean"))
opa_raw   <- fread(p_product(cfg, "parcels_clean"))

assert_has_cols(xwalk,
  c("grantee_upper", "PID", "owner_std", "owner_group_id", "is_corp",
    "is_financial_intermediary", "owner_category"),
  "owner_linkage_xwalk")

rtt[, PID := normalize_pid(PID)]
rtt[, year := as.integer(year)]
rtt[, grantee_upper := toupper(trimws(as.character(grantees)))]

opa_raw[, PID := normalize_pid(parcel_number)]

xwalk[, PID := normalize_pid(PID)]

# Concatenate owner_1 + owner_2: OPA uses a fixed-width field where long entity names
# overflow into owner_2. This handles both corporate truncation ("SARAH ALLEN COMMUNITY" +
# "HOMES V LP") and joint-person ownership ("GANGEMI JOSEPH A AND" + "CAROLE A H/W").
# For joint owners the concatenated string becomes the OPA entity name; the Phase 3
# name-similarity gate handles matching to RTT grantees correctly in both cases.
opa_raw[, owner_full := {
  o1 <- trimws(as.character(owner_1))
  o2 <- trimws(as.character(coalesce(owner_2, "")))
  fifelse(nchar(o2) > 0L, paste(o1, o2), o1)
}]
n_owner2_nonempty <- opa_raw[trimws(as.character(coalesce(owner_2, ""))) != "", .N]
logf("  OPA rows with non-empty owner_2 (concatenated): ", n_owner2_nonempty,
     log_file = log_file)

# Keep one OPA row per PID (most recently assessed)
if ("assessment_date" %in% names(opa_raw)) {
  setorder(opa_raw, PID, assessment_date)
  opa <- opa_raw[, .SD[.N], by = PID]
} else {
  opa <- unique(opa_raw, by = "PID")
}

opa[, owner_1_upper := toupper(trimws(owner_full))]

logf("  rtt_clean:     ", nrow(rtt), " rows, ", rtt[, uniqueN(PID)], " unique PIDs",
     log_file = log_file)
logf("  parcels_clean: ", nrow(opa), " unique PIDs", log_file = log_file)
logf("  owner_linkage_xwalk: ", nrow(xwalk), " rows, ",
     xwalk[, uniqueN(owner_group_id)], " Phase 1 entities", log_file = log_file)

# Occupancy panel: PID × year total_units (imputed, capped) for portfolio unit totals.
occ_panel <- fread(p_product(cfg, "parcel_occupancy_panel"),
  select = c("PID", "year", "total_units"))
occ_panel[, PID  := normalize_pid(as.character(PID))]
occ_panel[, year := as.integer(year)]
# Treat 0 or missing units as NA so they don't suppress legitimate zeroes in sums
occ_panel[is.na(total_units) | total_units <= 0, total_units := NA_real_]
logf("  parcel_occupancy_panel: ", nrow(occ_panel), " rows, ",
     occ_panel[, uniqueN(PID)], " PIDs", log_file = log_file)

# ============================================================
# SECTION 1: Normalize OPA owner names
# ============================================================
logf("--- SECTION 1: OPA name normalization ---", log_file = log_file)

opa[, is_corp_opa := str_detect(owner_1_upper, business_regex)]
opa[is_corp_opa == TRUE,  owner_std_opa := standardize_corp_name(owner_1_upper)]
opa[is_corp_opa == FALSE, owner_std_opa := owner_1_upper]
opa[owner_std_opa == "" | is.na(owner_std_opa), owner_std_opa := owner_1_upper]

logf("  OPA unique owner names:        ", opa[, uniqueN(owner_1_upper)],
     log_file = log_file)
logf("  OPA unique standardized names: ", opa[, uniqueN(owner_std_opa)],
     log_file = log_file)

# ============================================================
# SECTION 2: OPA name mailing address normalization
# ============================================================
logf("--- SECTION 2: Mailing address normalization ---", log_file = log_file)

normalize_mailing_addr <- function(addr1, zip) {
  addr <- toupper(trimws(coalesce(as.character(addr1), "")))
  # Strip trailing unit/suite/floor designations (common in commercial addresses)
  addr <- str_remove(addr,
    "(?i)\\s+(STE|SUITE|APT|UNIT|DEPT|#|FL(?:OOR)?|ROOM?|RM|PMB|PO\\s*BOX)\\s*\\S*\\s*$")
  addr <- str_squish(addr)
  zip_clean <- str_extract(
    toupper(trimws(coalesce(as.character(zip), ""))), "^\\d{5}")
  paste0(addr, "||", coalesce(zip_clean, "NOZIP"))
}
opa[,mailing_street := fifelse(mailing_street == "", NA_character_, mailing_street)]
opa[,mailing_zip_norm := mailing_zip |> str_pad(5, "left", "0") |> str_sub(1,5)]
opa[, mailing_norm := normalize_mailing_addr(mailing_street, mailing_zip_norm)]

# Filter out empty/trivial mailing addresses
opa[mailing_norm == "||NOZIP" | nchar(mailing_norm) <= 7, mailing_norm := NA_character_]

logf("  OPA rows with usable mailing address: ",
     opa[!is.na(mailing_norm), .N], " / ", nrow(opa), log_file = log_file)

# ============================================================
# SECTION 3: Build entity registry (Phase 1 + OPA additions)
# ============================================================
logf("--- SECTION 3: Entity registry ---", log_file = log_file)

# Phase 1 entities: one record per entity_id (= owner_group_id)
# Carry is_financial_intermediary and is_trust through from xwalk.
# xwalk may not have is_trust if produced by an older build-owner-linkage.R run;
# default to FALSE in that case.
if (!"is_trust" %in% names(xwalk)) xwalk[, is_trust := FALSE]
p1_entity <- unique(xwalk[, .(
  entity_id                 = owner_group_id,
  entity_name               = owner_std,
  is_corp                   = is_corp,
  is_trust                  = is_trust,
  is_financial_intermediary = is_financial_intermediary,
  owner_category            = owner_category
)])[, .SD[1L], by = entity_id]

max_p1_entity_id <- p1_entity[, max(entity_id, na.rm = TRUE)]

# OPA entities: match owner_std_opa against Phase 1 owner_std.
# Aggregate by owner_std_opa; take is_corp_opa = TRUE if any row says TRUE
# (same standardized name can appear with both TRUE/FALSE if business_regex
# partially matches due to word-boundary differences).
opa_std_uniq <- opa[, .(is_corp_opa = any(is_corp_opa, na.rm = TRUE)), by = owner_std_opa]

# Classify financial intermediaries and trusts for OPA-only entities
opa_std_uniq[, is_fi_opa := str_detect(
  coalesce(owner_std_opa, ""),
  financial_intermediary_regex
)]
opa_std_uniq[, is_trust_opa := str_detect(
  coalesce(owner_std_opa, ""),
  .trust_regex
)]

# Phase 1 name map for CORPS: one row per owner_std (PID=NA in xwalk means corp/FI).
p1_corp_map <- unique(xwalk[is.na(PID), .(owner_std, entity_id = owner_group_id,
                                            is_corp, is_financial_intermediary)])
# Guard: same owner_std mapping to multiple entity_ids shouldn't happen for corps.
p1_corp_map <- p1_corp_map[, .(
  entity_id                 = min(entity_id),
  is_corp                   = is_corp[1L],
  is_financial_intermediary = any(is_financial_intermediary, na.rm = TRUE)
), by = owner_std]

# Phase 1 name map for PERSONS: one row per (owner_std, PID).
p1_pers_map <- unique(xwalk[!is.na(PID), .(owner_std, PID,
                                             entity_id = owner_group_id,
                                             is_corp, is_financial_intermediary)])

# --- Corp OPA matching: by owner_std_opa alone ---
opa_corp_std <- opa_std_uniq[is_corp_opa == TRUE]
opa_corp_std <- merge(
  opa_corp_std,
  p1_corp_map[, .(owner_std, entity_id, is_corp_p1 = is_corp,
                   is_fi_p1 = is_financial_intermediary)],
  by.x = "owner_std_opa", by.y = "owner_std",
  all.x = TRUE
)

# --- Person OPA matching: by (owner_std_opa, PID) ---
# Join opa person rows to get (owner_std_opa, PID) → entity_id from Phase 1.
# Unmatched person rows (name mismatch) fall through to new-entity assignment.
# Phase 3 RTT-OPA transitive edges will close the gap for renamed/mismatched owners.
opa_pers_pid <- opa[is_corp_opa == FALSE, .(PID, owner_std_opa, is_corp_opa)]
opa_pers_pid <- merge(
  opa_pers_pid,
  p1_pers_map[, .(owner_std, PID, entity_id, is_corp_p1 = is_corp,
                   is_fi_p1 = is_financial_intermediary)],
  by.x = c("owner_std_opa", "PID"), by.y = c("owner_std", "PID"),
  all.x = TRUE
)
# Roll up person matches back to owner_std_opa level for new-entity logic.
# One owner_std_opa may match on some PIDs but not others; entity_id is PID-specific.
opa_pers_std <- opa_std_uniq[is_corp_opa == FALSE]
opa_pers_std <- merge(
  opa_pers_std,
  opa_pers_pid[!is.na(entity_id), .(
    entity_id                 = min(entity_id, na.rm = TRUE),
    is_corp_p1                = is_corp_p1[!is.na(entity_id)][1L],
    is_fi_p1                  = any(is_fi_p1[!is.na(entity_id)], na.rm = TRUE)
  ), by = owner_std_opa],
  by = "owner_std_opa", all.x = TRUE
)

# Combine corp and person blocks
opa_std_uniq <- rbind(opa_corp_std, opa_pers_std, fill = TRUE)

# New entity IDs for OPA owners not matched to Phase 1
n_new <- opa_std_uniq[is.na(entity_id), .N]
if (n_new > 0) {
  opa_std_uniq[is.na(entity_id),
    entity_id := seq(max_p1_entity_id + 1L, max_p1_entity_id + n_new)]
}
opa_std_uniq[is.na(is_corp_p1), is_corp_p1 := is_corp_opa]
opa_std_uniq[, is_corp := is_corp_opa | is_corp_p1]
opa_std_uniq[is.na(is_fi_p1), is_fi_p1 := is_fi_opa]
opa_std_uniq[, is_financial_intermediary := is_fi_opa | is_fi_p1]
opa_std_uniq[, is_trust := is_trust_opa]
opa_std_uniq[, c("is_corp_p1", "is_fi_p1", "is_fi_opa", "is_trust_opa") := NULL]

# owner_category for OPA-only entities (uses final is_corp/is_financial_intermediary/is_trust).
opa_std_uniq[, owner_category := assign_owner_category(
  owner_std_opa, is_corp, is_financial_intermediary, is_trust)]

logf("  Phase 1 entities: ", nrow(p1_entity), log_file = log_file)
logf("  New OPA-only entities: ", n_new, log_file = log_file)

# Attach entity_id to each OPA parcel.
# For persons, use the PID-specific entity_id from opa_pers_pid where available;
# fall back to the name-level match from opa_std_uniq for unmatched OPA persons.
opa <- merge(opa,
  opa_pers_pid[!is.na(entity_id), .(PID, entity_id_pers = entity_id)],
  by = "PID", all.x = TRUE)
opa <- merge(opa,
  opa_std_uniq[, .(owner_std_opa, entity_id_std = entity_id,
                    is_corp_opa_final = is_corp)],
  by = "owner_std_opa", all.x = TRUE)
opa[, entity_id := fifelse(
  is_corp_opa == FALSE & !is.na(entity_id_pers),
  entity_id_pers,
  entity_id_std
)]
opa[, c("entity_id_pers", "entity_id_std") := NULL]

# Full entity table (Phase 1 + OPA additions) — includes is_financial_intermediary
opa_new_entities <- opa_std_uniq[entity_id > max_p1_entity_id,
  .(entity_id, entity_name = owner_std_opa, is_corp, is_trust, is_financial_intermediary,
    owner_category)]
all_entities <- rbind(
  p1_entity,
  opa_new_entities[, .(entity_id, entity_name, is_corp, is_trust, is_financial_intermediary,
                        owner_category)],
  fill = TRUE
)
# Guard: p1_entity may not have is_trust if xwalk was missing it (already defaulted to FALSE above)
all_entities[is.na(is_trust), is_trust := FALSE]
all_entities <- unique(all_entities, by = "entity_id")

# Compute business_short_name for corp-to-corp Phase 2 matching.
# Strips common descriptor words (PROPERTIES, MANAGEMENT, REALTY, etc.) from entity_name
# so that "ABC PROPERTIES" and "ABC MANAGEMENT" at the same address are merged.
# Persons/trusts use entity_name directly (exact name + address match).
all_entities[, business_short_name := compute_business_short_name(
  coalesce(entity_name, "")
)]
# Blank short names (entity name was purely descriptor words) get set to NA
# so they don't accidentally match the empty-string of another entity.
all_entities[trimws(business_short_name) == "", business_short_name := NA_character_]

logf("  Total entities (Phase 1 + OPA additions): ", nrow(all_entities),
     log_file = log_file)

# ============================================================
# SECTION 4: Pre-compute RTT entity linkage (needed for Phase 3)
# ============================================================
logf("--- SECTION 4a: RTT entity pre-computation ---", log_file = log_file)

# Link RTT transfers to entity_id via Phase 1 crosswalk.
# Two-step: corps join by grantee_upper alone; persons join by (grantee_upper, PID).
# corp_grantees is stored here so it can be reused in SECTION 4c portfolio computation.
corp_grantees <- xwalk[is.na(PID), unique(grantee_upper)]

rtt_entity_corp <- merge(
  rtt[grantee_upper %in% corp_grantees, .(PID, year, display_date, grantee_upper)],
  xwalk[is.na(PID), .(grantee_upper, entity_id = owner_group_id)],
  by = "grantee_upper"
)
rtt_entity_pers <- merge(
  rtt[!grantee_upper %in% corp_grantees, .(PID, year, display_date, grantee_upper)],
  xwalk[!is.na(PID), .(grantee_upper, PID, entity_id = owner_group_id)],
  by = c("grantee_upper", "PID")
)
rtt_entity <- rbind(rtt_entity_corp, rtt_entity_pers)
rtt_entity <- rtt_entity[!is.na(entity_id)]

logf("  RTT rows with entity_id: ", nrow(rtt_entity),
     " (", rtt_entity[, uniqueN(PID)], " PIDs)", log_file = log_file)

# Last RTT grantee per PID (before 2025): used for Phase 3 RTT-OPA transitive edges
rtt_last_by_pid <- rtt_entity[year < 2025L]
setorder(rtt_last_by_pid, PID, display_date)
rtt_last_by_pid <- rtt_last_by_pid[, .SD[.N], by = PID]
rtt_last_by_pid <- rtt_last_by_pid[!is.na(entity_id), .(PID, entity_id)]
assert_unique(rtt_last_by_pid, "PID", "rtt_last_by_pid")

# OPA 2024 current owner per PID (static input for Phase 3)
opa_pid_entity_static <- opa[!is.na(entity_id), .(PID, entity_id)]
n_opa_dups_p3 <- opa_pid_entity_static[, .N, by = PID][N > 1L, .N]
if (n_opa_dups_p3 > 0) {
  logf("  WARNING: ", n_opa_dups_p3, " OPA PIDs have multiple entity_ids; taking first",
       log_file = log_file)
  opa_pid_entity_static <- opa_pid_entity_static[, .SD[1L], by = PID]
}
assert_unique(opa_pid_entity_static, "PID", "opa_pid_entity_static")

# Financial intermediary entity IDs (static throughout iteration)
fi_entity_ids <- all_entities[is_financial_intermediary == TRUE, entity_id]
logf("  Financial intermediary entity IDs: ", length(fi_entity_ids),
     log_file = log_file)

# ============================================================
# SECTION 4b: Conglomerate formation — Phase 2 (OPA-only) then Phase 3 (one-shot)
# ============================================================
# Design rationale:
#   Phase 2 runs to convergence using only OPA mailing-address data. Address profiles
#   are expanded across merged conglomerate members each round (enabling A↔B↔C chains
#   entirely within the parcel data). Phase 3 then adds RTT-OPA transitive edges ONCE
#   without triggering further Phase 2 expansion — preventing the chain explosion caused
#   by RTT entities' addresses re-entering the OPA matching graph.
# ============================================================
logf("--- SECTION 4b: Entity linkage ---", log_file = log_file)

DIAG_THRESHOLD <- as.integer(cfg$run$entity_linkage_diag_threshold %||% 20L)

# Raw OPA mailing-address profile (never polluted by RTT-only entity addresses)
opa_mailing_raw <- unique(opa[!is.na(entity_id) & !is.na(mailing_norm),
                               .(entity_id, mailing_norm)])

# ----------------------------------------------------------------
# CONSERVATIVE BASELINE SNAPSHOT
# Run ONE round of Phase 2 on the raw OPA profile (no expansion).
# Logs the conglomerate size distribution for comparison with the
# full iterative approach. Results are written to a QA CSV.
# ----------------------------------------------------------------
logf("  --- Conservative baseline (one round, no expansion) ---", log_file = log_file)
p2_conservative_edges <- build_name_address_edges(
  opa_mailing_raw, all_entities, fi_entity_ids,
  ra_threshold = REGISTERED_AGENT_THRESHOLD,
  log_file     = log_file
)
if (nrow(p2_conservative_edges) > 0L) {
  cons_verts <- unique(c(as.character(all_entities$entity_id),
                         as.character(p2_conservative_edges$entity_id_a),
                         as.character(p2_conservative_edges$entity_id_b)))
  cons_g <- igraph::graph_from_data_frame(
    data.frame(from = as.character(p2_conservative_edges$entity_id_a),
               to   = as.character(p2_conservative_edges$entity_id_b),
               stringsAsFactors = FALSE),
    directed = FALSE, vertices = cons_verts
  )
  cons_comps <- igraph::components(cons_g)
  cons_dt <- data.table(
    entity_id = as.integer(names(cons_comps$membership)),
    component = cons_comps$membership
  )
  cons_dt[, cong_size := .N, by = component]
  cons_dt[, conglomerate_id := min(entity_id), by = component]
  n_cons_congs  <- cons_dt[, uniqueN(conglomerate_id)]
  n_cons_gt5    <- cons_dt[cong_size >   5L, uniqueN(conglomerate_id)]
  n_cons_gt20   <- cons_dt[cong_size >  20L, uniqueN(conglomerate_id)]
  n_cons_gt100  <- cons_dt[cong_size > 100L, uniqueN(conglomerate_id)]
  logf("  CONSERVATIVE: ", n_cons_congs, " conglomerates | >5: ", n_cons_gt5,
       " | >20: ", n_cons_gt20, " | >100: ", n_cons_gt100, log_file = log_file)
  # Top 20 by size
  cons_top20 <- cons_dt[, .(n_entities = .N), by = conglomerate_id
                        ][order(-n_entities)][1:min(.N, 20L)]
  cons_top20 <- merge(
    cons_top20,
    all_entities[, .(entity_id, entity_name)],
    by.x = "conglomerate_id", by.y = "entity_id", all.x = TRUE
  )
  logf("  Top 10 conservative conglomerates by size:", log_file = log_file)
  for (i in seq_len(min(nrow(cons_top20), 10L))) {
    logf("    ", coalesce(cons_top20$entity_name[i], "(unknown)"), ": ",
         cons_top20$n_entities[i], " entities", log_file = log_file)
  }
  fwrite(cons_top20,
         file.path(p_out(cfg, "qa"), "ownership_conservative_top_conglomerates.csv"))
  logf("  Wrote QA: ownership_conservative_top_conglomerates.csv", log_file = log_file)
} else {
  logf("  CONSERVATIVE: no Phase 2 edges (all singletons)", log_file = log_file)
}

# ----------------------------------------------------------------
# PHASE 2: OPA-only mailing-address loop — run to convergence
# Address profiles expand across merged conglomerate members each
# round, enabling full transitive closure within the parcel data.
# RTT-only entities have no OPA mailing rows, so they cannot
# contribute addresses here even if they appear in entity_conglomerate.
# ----------------------------------------------------------------
logf("  --- Phase 2: OPA-only mailing-address (max ", MAX_ROUNDS, " rounds) ---",
     log_file = log_file)

entity_mailing_profile <- copy(opa_mailing_raw)
entity_conglomerate <- data.table(
  entity_id       = all_entities$entity_id,
  conglomerate_id = all_entities$entity_id
)
p2_prev_edges <- data.table(entity_id_a = integer(0), entity_id_b = integer(0))
all_p2_entities <- integer(0)

# first_merged_round tracks when each entity first joined a multi-entity conglomerate.
# Will be extended for Phase 3 merges below (using round = MAX_ROUNDS + 1 as a sentinel).
first_merged_round <- rep(NA_integer_, nrow(all_entities))
names(first_merged_round) <- as.character(all_entities$entity_id)

for (round in seq_len(MAX_ROUNDS)) {
  logf("    Round ", round, log_file = log_file)

  p2_edges <- build_name_address_edges(
    entity_mailing_profile, all_entities, fi_entity_ids,
    ra_threshold = REGISTERED_AGENT_THRESHOLD,
    log_file     = log_file
  )
  if (nrow(p2_edges) > 0L) {
    all_p2_entities <- unique(c(all_p2_entities,
                                p2_edges$entity_id_a, p2_edges$entity_id_b))
  }

  all_p2_edges <- unique(rbind(p2_prev_edges, p2_edges))
  n_new_p2 <- nrow(all_p2_edges) - nrow(p2_prev_edges)
  logf("      Phase 2 new pairs: ", n_new_p2, log_file = log_file)

  if (n_new_p2 == 0L) {
    logf("    Phase 2 converged at round ", round, log_file = log_file)
    break
  }

  # Rebuild components from Phase 2 edges only
  p2_verts <- unique(c(as.character(all_entities$entity_id),
                       as.character(all_p2_edges$entity_id_a),
                       as.character(all_p2_edges$entity_id_b)))
  g_p2 <- igraph::graph_from_data_frame(
    data.frame(from = as.character(all_p2_edges$entity_id_a),
               to   = as.character(all_p2_edges$entity_id_b),
               stringsAsFactors = FALSE),
    directed = FALSE, vertices = p2_verts
  )
  comps_p2 <- igraph::components(g_p2)
  comp_p2_dt <- data.table(
    entity_id = as.integer(names(comps_p2$membership)),
    component = comps_p2$membership
  )
  comp_p2_dt[, conglomerate_id := min(entity_id), by = component]
  entity_conglomerate <- comp_p2_dt[, .(entity_id, conglomerate_id)]

  n_p2_congs  <- entity_conglomerate[, uniqueN(conglomerate_id)]
  n_p2_merged <- entity_conglomerate[conglomerate_id != entity_id, .N]
  logf("      Conglomerates: ", n_p2_congs, "  Entities merged: ", n_p2_merged,
       log_file = log_file)

  newly_merged_ids <- entity_conglomerate[
    conglomerate_id != entity_id &
      is.na(first_merged_round[as.character(entity_id)]),
    entity_id
  ]
  first_merged_round[as.character(newly_merged_ids)] <- round

  # Expand mailing profile: each entity inherits all OPA addresses of its conglomerate.
  # Only opa_mailing_raw addresses (pure OPA data) are used — no RTT addresses can enter.
  opa_with_cong <- merge(
    opa_mailing_raw,
    entity_conglomerate[, .(entity_id, conglomerate_id)],
    by = "entity_id", all.x = TRUE
  )
  opa_with_cong[is.na(conglomerate_id), conglomerate_id := entity_id]
  cong_addresses <- unique(opa_with_cong[, .(conglomerate_id, mailing_norm)])
  cong_members   <- entity_conglomerate[, .(entity_id, conglomerate_id)]
  entity_mailing_profile <- unique(merge(
    cong_members, cong_addresses, by = "conglomerate_id",
    allow.cartesian = TRUE  # intentional: each entity inherits all conglomerate addresses
  )[, .(entity_id, mailing_norm)])

  p2_prev_edges <- all_p2_edges

  if (round == MAX_ROUNDS) {
    logf("    Reached max rounds (", MAX_ROUNDS, ") without Phase 2 convergence.",
         log_file = log_file)
  }
}

# Summary after Phase 2
n_p2_final_congs <- entity_conglomerate[, uniqueN(conglomerate_id)]
n_p2_gt5   <- entity_conglomerate[, .N, by = conglomerate_id][N >   5L, .N]
n_p2_gt20  <- entity_conglomerate[, .N, by = conglomerate_id][N >  20L, .N]
n_p2_gt100 <- entity_conglomerate[, .N, by = conglomerate_id][N > 100L, .N]
logf("  Phase 2 FINAL: ", n_p2_final_congs, " conglomerates | >5: ", n_p2_gt5,
     " | >20: ", n_p2_gt20, " | >100: ", n_p2_gt100, log_file = log_file)
logf("  Total Phase 2 edges: ", nrow(p2_prev_edges), log_file = log_file)

# ----------------------------------------------------------------
# BRIDGE-ADDRESS DIAGNOSTICS: for large Phase 2 conglomerates,
# log which mailing addresses connected the most entities.
# ----------------------------------------------------------------
cong_sizes_p2 <- entity_conglomerate[, .(n_members = .N), by = conglomerate_id]
large_cong_ids <- cong_sizes_p2[n_members > DIAG_THRESHOLD, conglomerate_id]
if (length(large_cong_ids) > 0L) {
  logf("  Bridge-address diagnostics (conglomerates > ", DIAG_THRESHOLD, " members):",
       log_file = log_file)
  # Use opa_mailing_raw (original, pre-pooling) so we see the ACTUAL addresses each
  # entity originally had, not the pooled addresses assigned after convergence.
  # n_entities_at_addr = how many conglomerate members originally listed this address.
  bridge_dt <- merge(
    entity_conglomerate[conglomerate_id %in% large_cong_ids,
                        .(entity_id, conglomerate_id)],
    opa_mailing_raw,   # original OPA mailing data — NOT the pooled profile
    by = "entity_id"
  )
  # Per (conglomerate_id, mailing_norm): how many conglomerate members share this address?
  bridge_summary <- bridge_dt[,
    .(n_entities_at_addr = uniqueN(entity_id)),
    by = .(conglomerate_id, mailing_norm)
  ]
  # Attach conglomerate entity_name for readability
  bridge_summary <- merge(
    bridge_summary,
    all_entities[, .(entity_id, entity_name)],
    by.x = "conglomerate_id", by.y = "entity_id", all.x = TRUE
  )
  bridge_summary <- merge(bridge_summary, cong_sizes_p2, by = "conglomerate_id")
  setorder(bridge_summary, -n_members, conglomerate_id, -n_entities_at_addr)

  # Log top bridge addresses per conglomerate (up to 5 per conglomerate, top 10 conglomerates)
  top_large_cong <- unique(bridge_summary$conglomerate_id)[1:min(length(unique(bridge_summary$conglomerate_id)), 10L)]
  for (cid in top_large_cong) {
    crows <- bridge_summary[conglomerate_id == cid][1:min(.N, 5L)]
    logf("    [", cid, "] ", coalesce(crows$entity_name[1L], "(unknown)"),
         " (", crows$n_members[1L], " members) — top addresses:",
         log_file = log_file)
    for (j in seq_len(nrow(crows))) {
      logf("      ", crows$mailing_norm[j], " → ", crows$n_entities_at_addr[j], " entities",
           log_file = log_file)
    }
  }

  fwrite(bridge_summary,
         file.path(p_out(cfg, "qa"), "ownership_large_conglomerate_bridge_addresses.csv"))
  logf("  Wrote QA: ownership_large_conglomerate_bridge_addresses.csv", log_file = log_file)
}

# ----------------------------------------------------------------
# PHASE 3: RTT-OPA transitive edges — ONE SHOT, no re-expansion
# If A=B=C in OPA (Phase 2) and C=D in RTT, and C and D share a
# plausible name similarity, then A=B=C=D — stop there.
# Name gate prevents false merges where City/PHA/RedDev previously
# owned parcels later conveyed to unrelated private entities.
# ----------------------------------------------------------------
logf("  --- Phase 3: RTT-OPA transitive (one shot) ---", log_file = log_file)

rtt_mapped <- merge(
  rtt_last_by_pid,
  entity_conglomerate[, .(entity_id, conglomerate_id)],
  by = "entity_id", all.x = TRUE
)
rtt_mapped[is.na(conglomerate_id), conglomerate_id := entity_id]

opa_mapped <- merge(
  opa_pid_entity_static,
  entity_conglomerate[, .(entity_id, conglomerate_id)],
  by = "entity_id", all.x = TRUE
)
opa_mapped[is.na(conglomerate_id), conglomerate_id := entity_id]

p3_edges <- build_rtt_opa_edges(
  rtt_last_dt     = rtt_mapped[, .(PID, entity_id = conglomerate_id)],
  opa_dt          = opa_mapped[, .(PID, entity_id = conglomerate_id)],
  entity_attrs_dt = all_entities,
  fi_entity_ids   = fi_entity_ids,
  log_file        = log_file
)
logf("  Phase 3 edges (RTT-OPA transitive after name gate): ", nrow(p3_edges),
     log_file = log_file)

# Final: combine Phase 2 + Phase 3 edges and rebuild components
all_final_edges <- unique(rbind(p2_prev_edges, p3_edges))
if (nrow(all_final_edges) > 0L) {
  final_verts <- unique(c(as.character(all_entities$entity_id),
                          as.character(all_final_edges$entity_id_a),
                          as.character(all_final_edges$entity_id_b)))
  g_final <- igraph::graph_from_data_frame(
    data.frame(from = as.character(all_final_edges$entity_id_a),
               to   = as.character(all_final_edges$entity_id_b),
               stringsAsFactors = FALSE),
    directed = FALSE, vertices = final_verts
  )
  comps_final <- igraph::components(g_final)
  comp_final_dt <- data.table(
    entity_id = as.integer(names(comps_final$membership)),
    component = comps_final$membership
  )
  comp_final_dt[, conglomerate_id := min(entity_id), by = component]
  entity_conglomerate <- comp_final_dt[, .(entity_id, conglomerate_id)]

  # Track Phase 3 merges in first_merged_round (sentinel round = MAX_ROUNDS + 1)
  p3_newly_merged <- entity_conglomerate[
    conglomerate_id != entity_id &
      is.na(first_merged_round[as.character(entity_id)]),
    entity_id
  ]
  first_merged_round[as.character(p3_newly_merged)] <- MAX_ROUNDS + 1L
}

logf("  Final: ", entity_conglomerate[, uniqueN(conglomerate_id)],
     " conglomerates from ", nrow(all_entities), " entities",
     log_file = log_file)
logf("  Total edges (Phase 2 + Phase 3): ", nrow(all_final_edges), log_file = log_file)

# Build consolidation_round lookup
consolidation_dt <- data.table(
  entity_id           = as.integer(names(first_merged_round)),
  consolidation_round = unname(first_merged_round)
)

# Merge conglomerate assignment back to all_entities (include is_trust)
all_entities_cong <- merge(
  all_entities[, .(entity_id, entity_name, is_corp, is_trust, is_financial_intermediary,
                   owner_category)],
  entity_conglomerate[, .(entity_id, conglomerate_id)],
  by = "entity_id", all.x = TRUE
)
all_entities_cong[is.na(conglomerate_id), conglomerate_id := entity_id]
all_entities_cong <- merge(all_entities_cong, consolidation_dt,
                            by = "entity_id", all.x = TRUE)

p1_merged <- all_entities_cong[entity_id <= max_p1_entity_id &
                                 conglomerate_id != entity_id, .N]
logf("  Phase 1 entities merged by linkage: ", p1_merged, log_file = log_file)

# ============================================================
# SECTION 5: Build xwalk_entity_conglomerate
# ============================================================
logf("--- SECTION 5: xwalk_entity_conglomerate ---", log_file = log_file)

all_entities_cong[, n_entities_in_conglomerate :=
  uniqueN(entity_id), by = conglomerate_id]

# link_method:
#   "exact_norm"      = singleton (never linked to anyone)
#   "mailing_address" = linked via Phase 2 OPA mailing-address graph
#   "rtt_transitive"  = linked solely via Phase 3 RTT-OPA transitive edge
#   (Phase 3 merges are recorded with consolidation_round = MAX_ROUNDS + 1)
all_entities_cong[, link_method := fcase(
  n_entities_in_conglomerate == 1L,                              "exact_norm",
  entity_id %in% all_p2_entities,                                "mailing_address",
  n_entities_in_conglomerate > 1L & !is.na(consolidation_round), "rtt_transitive",
  default = "exact_norm"
)]

assert_unique(all_entities_cong, "entity_id", "xwalk_entity_conglomerate")

xwalk_ec <- all_entities_cong[, .(
  entity_id, entity_name_clean = entity_name, is_corp, is_trust, is_financial_intermediary,
  owner_category,
  conglomerate_id, n_entities_in_conglomerate, link_method, consolidation_round
)]

logf("  xwalk_entity_conglomerate rows: ", nrow(xwalk_ec), log_file = log_file)
logf("  link_method distribution:", log_file = log_file)
lm_dist <- xwalk_ec[, .N, by = link_method]
for (i in seq_len(nrow(lm_dist))) {
  logf("    ", lm_dist$link_method[i], ": ", lm_dist$N[i], log_file = log_file)
}

# ----------------------------------------------------------------
# SPOT-CHECK QA: top N conglomerates — all member names + addresses
# One row per entity for the largest N conglomerates. Useful for
# manually verifying that Phase 2/3 merges look sensible.
# ----------------------------------------------------------------
SPOT_CHECK_N <- 100L

# n_parcels_opa: OPA parcel count per entity (pre-conglomerate, captures entity size)
entity_parcel_count <- opa[!is.na(entity_id), .(n_parcels_opa = .N), by = entity_id]

# Collapse original OPA mailing addresses per entity (top 3, semicolon-joined)
# Use opa_mailing_raw so each entity shows only its OWN original addresses —
# not the pooled addresses inherited from conglomerate merging.
entity_addrs_collapsed <- opa_mailing_raw[, .(
  opa_mailing_addrs = paste(head(unique(mailing_norm), 3L), collapse = "; ")
), by = entity_id]

# Top N conglomerates by member count
top_cong_ids <- xwalk_ec[, .(n = uniqueN(entity_id)), by = conglomerate_id
                         ][order(-n)][seq_len(min(.N, SPOT_CHECK_N)), conglomerate_id]

# Conglomerate representative name (entity whose entity_id == conglomerate_id)
cong_rep_names <- xwalk_ec[entity_id %in% top_cong_ids,
                            .(conglomerate_id = entity_id,
                              conglomerate_name = entity_name_clean)]

spot_check_dt <- xwalk_ec[conglomerate_id %in% top_cong_ids,
  .(conglomerate_id, n_entities_in_conglomerate,
    entity_id, entity_name_clean, is_corp, is_trust, link_method)
]
spot_check_dt <- merge(spot_check_dt,
  all_entities[, .(entity_id, business_short_name)],
  by = "entity_id", all.x = TRUE)
spot_check_dt <- merge(spot_check_dt, entity_parcel_count, by = "entity_id", all.x = TRUE)
spot_check_dt[is.na(n_parcels_opa), n_parcels_opa := 0L]
spot_check_dt <- merge(spot_check_dt, entity_addrs_collapsed, by = "entity_id", all.x = TRUE)
spot_check_dt <- merge(spot_check_dt, cong_rep_names, by = "conglomerate_id", all.x = TRUE)

setorder(spot_check_dt, -n_entities_in_conglomerate, conglomerate_id, entity_id)
setcolorder(spot_check_dt, c(
  "conglomerate_id", "conglomerate_name", "n_entities_in_conglomerate",
  "entity_id", "entity_name_clean", "is_corp", "is_trust", "business_short_name",
  "link_method", "n_parcels_opa", "opa_mailing_addrs"
))

fwrite(spot_check_dt,
       file.path(p_out(cfg, "qa"), "ownership_top_conglomerate_members.csv"))
logf("  Wrote QA: ownership_top_conglomerate_members.csv (",
     nrow(spot_check_dt), " rows across top ", SPOT_CHECK_N, " conglomerates)",
     log_file = log_file)

# ============================================================
# SECTION 4c: Portfolio construction (post-reconciliation, keyed on conglomerate_id)
# ============================================================
logf("--- SECTION 4c: Portfolio construction (conglomerate level) ---", log_file = log_file)

# Re-link all RTT transfers to entity_id using the same two-step join as SECTION 4a,
# then map entity_id → conglomerate_id via xwalk_ec.
rtt_all_entity <- rbind(
  merge(rtt[grantee_upper %in% corp_grantees,
            .(PID, year, is_sheriff_deed = grepl("SHERIFF", toupper(document_type)),
              grantee_upper)],
        xwalk[is.na(PID), .(grantee_upper, entity_id = owner_group_id)],
        by = "grantee_upper"),
  merge(rtt[!grantee_upper %in% corp_grantees,
            .(PID, year, is_sheriff_deed = grepl("SHERIFF", toupper(document_type)),
              grantee_upper)],
        xwalk[!is.na(PID), .(grantee_upper, PID, entity_id = owner_group_id)],
        by = c("grantee_upper", "PID"))
)
rtt_all_entity <- rtt_all_entity[!is.na(entity_id)]

rtt_all_cong <- merge(
  rtt_all_entity[, .(PID, year, entity_id, is_sheriff_deed)],
  xwalk_ec[, .(entity_id, conglomerate_id)],
  by = "entity_id"
)

portfolio_cong <- rtt_all_cong[, .(
  n_properties_total       = uniqueN(PID),
  n_properties_non_sheriff = uniqueN(PID[is_sheriff_deed == FALSE]),
  n_transfers              = .N,
  first_acquisition_year   = min(year, na.rm = TRUE),
  last_acquisition_year    = max(year, na.rm = TRUE)
), by = conglomerate_id]

portfolio_cong[, portfolio_bin := fcase(
  n_properties_non_sheriff == 0L,   "Sheriff-only",
  n_properties_non_sheriff == 1L,   "Single-purchase",
  n_properties_non_sheriff <= 4L,   "2-4",
  n_properties_non_sheriff <= 9L,   "5-9",
  n_properties_non_sheriff >= 10L,  "10+"
)]

fi_cong_ids <- xwalk_ec[is_financial_intermediary == TRUE, unique(conglomerate_id)]
portfolio_cong[conglomerate_id %in% fi_cong_ids, portfolio_bin := "Financial-Intermediary"]

fwrite(portfolio_cong, p_product(cfg, "owner_portfolio"))
logf("  owner_portfolio (conglomerate-level): ", nrow(portfolio_cong), " rows, ",
     portfolio_cong[, uniqueN(portfolio_bin)], " bins", log_file = log_file)

port_dist_cong <- portfolio_cong[, .N, by = portfolio_bin][order(portfolio_bin)]
for (i in seq_len(nrow(port_dist_cong))) {
  logf("    ", port_dist_cong$portfolio_bin[i], ": ",
       port_dist_cong$N[i], " conglomerates", log_file = log_file)
}

# ============================================================
# SECTION 6: PID × year ownership panel (entity level)
# ============================================================
logf("--- SECTION 6: PID x year ownership panel ---", log_file = log_file)

# rtt_entity was computed in SECTION 4a (pulled up for Phase 3 inputs).
# Within each (PID, year), take the last transfer by display_date.
setorder(rtt_entity, PID, year, display_date)
rtt_lastyear <- rtt_entity[, .SD[.N], by = .(PID, year)]
rtt_lastyear[, acquisition_year := year]

# PID × year grid
all_pids <- unique(c(rtt$PID, opa$PID))
pid_year <- CJ(PID = all_pids, year = YEAR_MIN:YEAR_MAX)

logf("  PID x year grid: ", nrow(pid_year), " rows (",
     length(all_pids), " PIDs x ", YEAR_MAX - YEAR_MIN + 1L, " years)",
     log_file = log_file)

# Rolling join: for each (PID, year), find most recent RTT transfer <= year
setkey(rtt_lastyear, PID, year)
setkey(pid_year,     PID, year)
own_panel <- rtt_lastyear[, .(PID, year, entity_id, acquisition_year)][
  pid_year, roll = TRUE
]

# has_rtt_owner = TRUE if entity came from RTT chain in this row
own_panel[, has_rtt_owner := !is.na(entity_id)]

# OPA fallback: for rows still with NA entity_id, use OPA owner.
# opa_pid_entity_static already computed and deduplicated in SECTION 4a.
own_panel <- merge(own_panel,
  opa_pid_entity_static[, .(PID, entity_id_opa = entity_id)],
  by = "PID", all.x = TRUE)
own_panel[is.na(entity_id) & !is.na(entity_id_opa), entity_id := entity_id_opa]
own_panel[, entity_id_opa := NULL]

# is_corp_owner and is_financial_intermediary_owner
own_panel <- merge(
  own_panel,
  all_entities_cong[, .(entity_id, is_corp, is_financial_intermediary, owner_category)],
  by = "entity_id", all.x = TRUE
)
setnames(own_panel, "is_corp",                   "is_corp_owner")
setnames(own_panel, "is_financial_intermediary",  "is_financial_intermediary_owner")
own_panel[is.na(is_financial_intermediary_owner), is_financial_intermediary_owner := FALSE]

logf("  Rows with is_financial_intermediary_owner == TRUE: ",
     own_panel[is_financial_intermediary_owner == TRUE, .N],
     " (", round(100 * own_panel[is_financial_intermediary_owner == TRUE, .N] /
                   nrow(own_panel), 1), "%)",
     log_file = log_file)

# years_since_acquisition
own_panel[has_rtt_owner == TRUE, years_since_acquisition := year - acquisition_year]

# ============================================================
# SECTION 7: Time-varying entity portfolio size
# ============================================================
logf("--- SECTION 7: Time-varying portfolio sizes ---", log_file = log_file)

port_entity <- own_panel[!is.na(entity_id),
  .(portfolio_size_entity_yr = uniqueN(PID)),
  by = .(entity_id, year)
]

# Total units owned per entity×year: sum total_units across all PIDs in the portfolio.
# Source: parcel_occupancy_panel (imputed, capped units per PID×year).
port_entity_units <- merge(
  own_panel[!is.na(entity_id), .(entity_id, PID, year)],
  occ_panel[, .(PID, year, total_units)],
  by = c("PID", "year"), all.x = TRUE
)[, .(total_units_entity_yr = sum(total_units, na.rm = TRUE)),
  by = .(entity_id, year)]
port_entity <- merge(port_entity, port_entity_units, by = c("entity_id", "year"), all.x = TRUE)

own_panel <- merge(own_panel, port_entity, by = c("entity_id", "year"), all.x = TRUE)

own_panel[, portfolio_bin_entity_yr := fcase(
  is.na(portfolio_size_entity_yr),  NA_character_,
  portfolio_size_entity_yr == 1L,   "1",
  portfolio_size_entity_yr <= 4L,   "2-4",
  portfolio_size_entity_yr <= 19L,  "5-19",
  portfolio_size_entity_yr >= 20L,  "20+"
)]

setorder(own_panel, PID, year)

assert_unique(own_panel, c("PID", "year"), "xwalk_pid_entity")

logf("  xwalk_pid_entity rows: ", nrow(own_panel), log_file = log_file)
logf("  Rows with entity_id: ",  own_panel[!is.na(entity_id), .N],
     " (", round(100 * own_panel[!is.na(entity_id), .N] / nrow(own_panel), 1), "%)",
     log_file = log_file)
logf("  RTT-sourced rows:     ", own_panel[has_rtt_owner == TRUE, .N],
     " (", round(100 * own_panel[has_rtt_owner == TRUE, .N] / nrow(own_panel), 1), "%)",
     log_file = log_file)

# Portfolio size distribution (2011-2019 for reference)
port_ref <- own_panel[year %in% 2011:2019 & !is.na(portfolio_bin_entity_yr),
  .(n_pid_years = .N), by = portfolio_bin_entity_yr]
logf("  Portfolio bin distribution (2011-2019 PID-years):", log_file = log_file)
for (i in seq_len(nrow(port_ref))) {
  logf("    ", port_ref$portfolio_bin_entity_yr[i], ": ",
       port_ref$n_pid_years[i], log_file = log_file)
}

# ============================================================
# SECTION 8: PID × year conglomerate panel
# ============================================================
logf("--- SECTION 8: PID x year conglomerate panel ---", log_file = log_file)

xwalk_pid_cong <- merge(
  own_panel[, .(PID, year, entity_id, has_rtt_owner)],
  xwalk_ec[, .(entity_id, conglomerate_id)],
  by = "entity_id", all.x = TRUE
)

# is_corp_conglomerate: TRUE if ANY entity in the conglomerate is corporate
# is_financial_intermediary_owner: TRUE if ANY entity in the conglomerate is FI
flags_by_cong <- xwalk_ec[, .(
  is_corp_conglomerate             = any(is_corp,                   na.rm = TRUE),
  is_financial_intermediary_owner  = any(is_financial_intermediary, na.rm = TRUE)
), by = conglomerate_id]
xwalk_pid_cong <- merge(xwalk_pid_cong, flags_by_cong, by = "conglomerate_id", all.x = TRUE)
xwalk_pid_cong[is.na(is_financial_intermediary_owner),
               is_financial_intermediary_owner := FALSE]

# Time-varying conglomerate portfolio size
port_cong <- xwalk_pid_cong[!is.na(conglomerate_id),
  .(portfolio_size_conglomerate_yr = uniqueN(PID)),
  by = .(conglomerate_id, year)
]

# Total units owned per conglomerate×year
port_cong_units <- merge(
  xwalk_pid_cong[!is.na(conglomerate_id), .(conglomerate_id, PID, year)],
  occ_panel[, .(PID, year, total_units)],
  by = c("PID", "year"), all.x = TRUE
)[, .(total_units_conglomerate_yr = sum(total_units, na.rm = TRUE)),
  by = .(conglomerate_id, year)]
port_cong <- merge(port_cong, port_cong_units, by = c("conglomerate_id", "year"), all.x = TRUE)

xwalk_pid_cong <- merge(xwalk_pid_cong, port_cong,
  by = c("conglomerate_id", "year"), all.x = TRUE)

xwalk_pid_cong[, portfolio_bin_conglomerate_yr := fcase(
  is.na(portfolio_size_conglomerate_yr),  NA_character_,
  portfolio_size_conglomerate_yr == 1L,   "1",
  portfolio_size_conglomerate_yr <= 4L,   "2-4",
  portfolio_size_conglomerate_yr <= 19L,  "5-19",
  portfolio_size_conglomerate_yr >= 20L,  "20+"
)]

setorder(xwalk_pid_cong, PID, year)
assert_unique(xwalk_pid_cong, c("PID", "year"), "xwalk_pid_conglomerate")

logf("  xwalk_pid_conglomerate rows: ", nrow(xwalk_pid_cong), log_file = log_file)

# ============================================================
# SECTION 9: Name lookup table
# ============================================================
logf("--- SECTION 9: Name lookup table ---", log_file = log_file)

# RTT names
# Corp/FI rows (PID=NA): one entity per grantee_upper — straightforward.
# Person rows (PID!=NA): multiple entity_ids per grantee_upper (one per PID).
# Collapse to min(entity_id) per name as a deterministic representative.
# Conglomerate lookup via xwalk_ec covers all a person's properties regardless of
# which entity_id is chosen here.
corp_rtt_name_dt <- xwalk[is.na(PID), .(
  name_raw    = grantee_upper,
  name_source = "rtt_grantee",
  entity_id   = owner_group_id,
  is_corp
)]
pers_rtt_name_dt <- xwalk[!is.na(PID),
  .(entity_id = min(owner_group_id), is_corp = FALSE),
  by = grantee_upper
][, .(name_raw = grantee_upper, name_source = "rtt_grantee", entity_id, is_corp)]
rtt_name_dt <- unique(rbind(corp_rtt_name_dt, pers_rtt_name_dt))

# OPA names
opa_name_dt <- unique(opa[!is.na(entity_id), .(
  name_raw    = owner_1_upper,
  name_source = "opa_owner_1",
  entity_id,
  is_corp     = is_corp_opa_final
)])

# Combine: mark "both" if name appears in both sources
name_lookup_raw <- rbind(rtt_name_dt, opa_name_dt, fill = TRUE)

# For each name_raw, collapse to one row (taking entity_id from RTT if conflict)
name_lookup_raw[, rtt_row := name_source == "rtt_grantee"]
setorder(name_lookup_raw, name_raw, -rtt_row)
name_lookup_raw <- name_lookup_raw[, {
  sources <- sort(unique(name_source))
  src_label <- if (length(sources) >= 2L) "both" else sources[1L]
  list(
    name_source = src_label,
    entity_id   = entity_id[1L],
    is_corp     = is_corp[1L]
  )
}, by = name_raw]

# Add conglomerate_id and owner_category
name_lookup_raw <- merge(
  name_lookup_raw,
  xwalk_ec[, .(entity_id, conglomerate_id, owner_category)],
  by = "entity_id", all.x = TRUE
)
# Singletons not in xwalk_ec (edge case: OPA-only entities not yet in all_entities_cong)
name_lookup_raw[is.na(conglomerate_id), conglomerate_id := entity_id]

assert_unique(name_lookup_raw, "name_raw", "name_lookup")

logf("  name_lookup rows: ", nrow(name_lookup_raw), log_file = log_file)
logf("  RTT-only: ", name_lookup_raw[name_source == "rtt_grantee", .N],
     "  OPA-only: ", name_lookup_raw[name_source == "opa_owner_1", .N],
     "  Both: ", name_lookup_raw[name_source == "both", .N],
     log_file = log_file)

# ============================================================
# SECTION 10: Write outputs
# ============================================================
logf("--- SECTION 10: Writing outputs ---", log_file = log_file)
# NOTE: owner_portfolio was already written in SECTION 4c (conglomerate-keyed).

fwrite(xwalk_ec,          p_product(cfg, "xwalk_entity_conglomerate"))
logf("  Wrote xwalk_entity_conglomerate: ", nrow(xwalk_ec), " rows", log_file = log_file)

fwrite(own_panel[, .(
  PID, year, entity_id, is_corp_owner, is_financial_intermediary_owner,
  owner_category,
  portfolio_size_entity_yr, portfolio_bin_entity_yr, total_units_entity_yr,
  years_since_acquisition, has_rtt_owner
)], p_product(cfg, "xwalk_pid_entity"))
logf("  Wrote xwalk_pid_entity: ", nrow(own_panel), " rows", log_file = log_file)

fwrite(xwalk_pid_cong[, .(
  PID, year, conglomerate_id, is_corp_conglomerate, is_financial_intermediary_owner,
  portfolio_size_conglomerate_yr, portfolio_bin_conglomerate_yr, total_units_conglomerate_yr,
  has_rtt_owner
)], p_product(cfg, "xwalk_pid_conglomerate"))
logf("  Wrote xwalk_pid_conglomerate: ", nrow(xwalk_pid_cong), " rows", log_file = log_file)

fwrite(name_lookup_raw, p_product(cfg, "name_lookup"))
logf("  Wrote name_lookup: ", nrow(name_lookup_raw), " rows", log_file = log_file)

# QA: top conglomerates by 2015 portfolio size
top_congs_2015 <- xwalk_pid_cong[year == 2015L & !is.na(conglomerate_id),
  .(n_pids = uniqueN(PID)), by = conglomerate_id
][order(-n_pids)][1:min(.N, 20L)]
top_congs_2015 <- merge(
  top_congs_2015,
  xwalk_ec[, .SD[1L], by = conglomerate_id][,
    .(conglomerate_id, entity_name_clean, n_entities_in_conglomerate, link_method)],
  by = "conglomerate_id", all.x = TRUE
)
qa_dir <- p_out(cfg, "qa")
fs::dir_create(qa_dir, recurse = TRUE)
fwrite(top_congs_2015, file.path(qa_dir, "ownership_top_conglomerates_2015.csv"))
logf("  Wrote QA: top conglomerates in 2015", log_file = log_file)

logf("=== build-ownership-panel.R complete ===", log_file = log_file)

# read back in all files
xwalk_ec_check <- fread(p_product(cfg, "xwalk_entity_conglomerate"))
own_panel_check <- fread(p_product(cfg, "xwalk_pid_entity"))
xwalk_pid_cong_check <- fread(p_product(cfg, "xwalk_pid_conglomerate"))
name_lookup_check <- fread(p_product(cfg, "name_lookup"))
top_congs_2015_check <- fread(file.path(qa_dir, "ownership_top_conglomerates_2015.csv"))




