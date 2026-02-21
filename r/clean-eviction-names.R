## ============================================================
## clean-eviction-names.R
## ============================================================
## Purpose: Clean and standardize party names from eviction records.
##          Identifies business vs. person names, standardizes spelling,
##          and performs fuzzy matching to link similar names.
##
## Inputs:
##   - cfg$inputs$evictions_party_names (evictions/phila-lt-data/party-names-addresses.txt)
##
## Outputs:
##   - cfg$products$eviction_names_clean (clean/eviction_names_clean.csv)
##
## Primary key: id + role (case ID and party role)
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(stringdist)
  library(igraph)
})

# ---- Load config and set up logging ----
source("r/config.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "clean-eviction-names.log")

logf("=== Starting clean-eviction-names.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ============================================================
# HELPER FUNCTIONS
# ============================================================

# Comprehensive list of business entity terms (with abbreviations, plurals, and variations)
business_words <- c(

"LLC", "L\\.L\\.C\\.", "LLCS",
"LIMITED PARTNERSHIP", "LTD", "L\\.T\\.D\\.", "LTDs",
"INC", "INC\\.", "INCS", "INCORPORATED",
"CORP", "CORPORATION", "CORPS",
"L\\.P\\.", "LP", "LPS",
"LLP", "LLPS",
"CO", "CO\\.", "COMPANY", "COMPANIES",
"HOLDING", "HOLDINGS",
"PARTNERSHIP", "PARTNERSHIPS", "PARTNER", "PARTNERS",
"ASSOC", "ASSOCS", "ASSOCIATES", "ASSOCIATION", "ASSOCIATIONS",
"ENTERPRISE", "ENTERPRISES",
"VENTURE", "VENTURES",
"GROUP", "GROUPS",
"SOLUTIONS", "STRATEGIES",
"BROS", "BROTHERS",
"FIRM", "FIRMS",
"TRUST", "TRUSTS",
"HOUS", "HOUSING", "APARTMENTS", "APTS?", "REAL",
"ESTATE", "REALTY", "MANAGEMENT", "MGMT",
"DEV", "DEVELOPMENT", "DEVELOPS", "DEVELOPERS",
"PHILADELPHIA", "SCHOOL", "SCHOOLS", "ACADEMY",
"TAVERN", "INN", "HOTEL", "MOTEL", "BAR", "GRILL", "DINER", "CAFE",
"UNIVERSITY", "COLLEGE", "HOSPITAL", "CLINIC",
"CHURCH", "TEMPLE", "MOSQUE", "MINISTRY", "MINISTRIES",
"DEPARTMENT", "DEPT", "AGENCY", "BUREAU", "OFFICE", "OFFICES",
"CITY", "COUNTY", "BOROUGH", "TOWNSHIP", "COMMONWEALTH", "STATE",
"BANK", "CREDIT", "UNION", "INSURANCE",
"LAW", "AUTO", "COLLISION", "COLLISSION", "MOTORS", "GARAGE",
"BODY", "REPAIR", "TOWING", "MECHANIC", "SERVICE", "SERVICES",
"THE"
)

# Create regex pattern (case insensitive)
pattern <- str_c("\\b(", str_c(business_words, collapse = "|"), ")\\b", collapse = "|")
occupant_token_pattern <- "(?:OCC(?!UPATIONAL\\b)[A-Z]{0,12})"
spousal_marker_regex <- regex(
  str_c(
    "\\b[HW]\\s*(?:AND|&|/)\\s*[HW]\\b",
    "|\\b[HW]\\s+[HW]\\b",
    "|\\(?\\s*C\\s*/\\s*S\\s*\\)?",
    "|\\bC\\s+S\\b",
    "|\\bHUSBAND\\s+AND\\s+WIFE\\b",
    "|\\bHUSBAND\\s*&\\s*WIFE\\b"
  ),
  ignore_case = TRUE
)

strip_spousal_phrases <- function(x) {
  x %>%
    str_replace_all(spousal_marker_regex, " ") %>%
    str_squish()
}

strip_occupant_phrases <- function(x) {
  x %>%
    str_replace_all(regex("\\bOCCU\\s+ANTS?\\b", ignore_case = TRUE), " ") %>%
    str_replace_all(
      regex("\\b(?:AND\\s+)?ALL\\s+OTHER?\\s+ROCC\\b", ignore_case = TRUE),
      " "
    ) %>%
    str_replace_all(
      regex(
        str_c(
          "\\b(?:AND\\s+)?(?:ALL\\s+)?(?:OTHER\\s+)?(?:UNAUTHORIZED\\s+)?(?:UNKNOWN\\s+)?",
          occupant_token_pattern,
          "\\b"
        ),
        ignore_case = TRUE
      ),
      " "
    ) %>%
    str_replace_all(regex("\\bET\\s+AL\\b", ignore_case = TRUE), " ") %>%
    str_squish()
}

# Function to fix common spelling errors and variations
fix_spellings <- function(x) {
x %>%
  str_replace_all("\\bA\\s?SSOCS\\b", "ASSOCIATES") %>%
  str_replace_all("\\bA\\s?A?SSOC[A-Z]+$", "ASSOCIATES") %>%
  str_replace_all("\\bASSOCI ATES\\b", "ASSOCIATES") %>%
  str_replace_all("\\bASSOC\\b", "ASSOCIATION") %>%
  str_replace_all("\\bASS\\sOC\\b", "ASSOCIATION") %>%
  str_replace_all("\\bAUTH\\b", "AUTHORITY") %>%
  str_replace_all("\\DEVEL\\b", "DEVELOP") %>%
  str_replace_all("\\REDEVEL\\b", "REDEVELOP") %>%
  str_replace_all("I AT", "IAT") %>%
  str_replace_all("\\bCOR\\s?P\\b", "CORPORATION") %>%
  str_replace_all("\\bCORPORAT ?ION\\b", "CORPORATION") %>%
  str_replace_all("\\bTERR\\b", "TERRACE") %>%
  str_replace_all("\\bAP T\\b", "APARTMENTS") %>%
  str_replace_all("\\bAPTS?\\b", "APARTMENTS") %>%
  str_replace_all("\\bAPART\\b", "APARTMENTS") %>%
  str_replace_all("\\bMGR\\b", "MANAGER") %>%
  str_replace_all("\\bPRTNRS\\b", "PARTNER") %>%
  str_replace_all("\\bPROP\\b", "PROPERTIES") %>%
  str_replace_all("\\bINV\\b", "INVESTMENTS") %>%
  str_replace_all("\\bREALTY\\b", "REAL ESTATE") %>%
  str_replace_all("\\bMGMT\\b", "MANAGEMENT") %>%
  str_replace_all("\\bDEVELOPS?\\b", "DEVELOPMENT") %>%
  str_replace_all("\\bDEV\\b", "DEVELOPMENT") %>%
  str_replace_all("\\bBLDG?\\b", "BUILDING") %>%
  str_replace_all("\\bHLDGS?\\b", "HOLDINGS") %>%
  str_replace_all("\\bMANO R\\b", "MANOR") %>%
  str_replace_all("\\bTR\\b", "TRUST") %>%
  str_replace_all("\\bGARDE ?N ?S\\b", "GARDENS") %>%
  str_replace_all("\\bPARTNRS\\b", "PARTNERS") %>%
  str_replace_all("\\bDUPLE X\\b", "DUPLEX") %>%
  str_replace_all("\\bPRTNRS?\\b", "PARTNERS") %>%
  str_replace_all("\\bPA?RTN?R\\b", "PARTNER") %>%
  str_replace_all("\\bPTSHP\\b", "PARTNERSHIP") %>%
  str_replace_all("\\ESQ\\b", "ESQUIRE") %>%
  str_replace_all("\\bINVESTMNTS?\\b", "INVESTMENT") %>%
  str_replace_all("\\bSCATTERED SITES?\\b", "SCATTERED HOUSING") %>%
  str_replace_all("\\bOWNERS?\\b", " ") %>%
  str_replace_all("\\bPHILA\\b", "PHILADELPHIA") %>%
  fifelse(
    str_detect(., "(PHILA.+HOU\\s?S|HOUS.+ AUTH?|PHILA.+AUTH|ADELPHIA HOUSE)"),
    "PHILADELPHIA HOUSING AUTHORITY", .
  ) %>%
  str_squish() %>%
  str_trim() %>%
  return()
}

clean_name <- function(name) {
name %>%
  str_replace_all("[’']", "") %>%
  strip_spousal_phrases() %>%
  # Replace & with AND
  str_replace_all("([^\\s])&([^\\s])", "\\1 AND \\2") %>%
  str_replace_all("([^\\s]),([^\\s])", "\\1 , \\2") %>%
  str_replace_all("&", " AND ") %>%
  str_replace_all("\\|", " | ") %>%
  # Strip punctuation
  str_replace_all("[[:punct:]]", " ") %>%
  str_squish() %>%
  strip_spousal_phrases() %>%
  strip_occupant_phrases() %>%
  fix_spellings() %>%
  strip_spousal_phrases() %>%
  strip_occupant_phrases() %>%
  str_squish() %>%
  return()
}

make_business_short_name <- function(x) {
x %>%
  # Remove business words or middle initials
  str_remove_all(regex(pattern, ignore_case = TRUE)) %>%
  # Remove leading "the"
  str_remove("^THE\\s") %>%
  # Remove trailing roman numerals
  str_remove("\\b(VI{0,3}|IV|IX|XI{0,3}|II|I|III)[\\b$]") %>%
  str_remove("\\b([0-9+])$") %>%
  str_squish() %>%
  return()
}

make_person_short_name <- function(x) {
x %>%
  # Remove middle initials, e.g., joe d fish -> joe fish
  str_remove_all("\\b[A-Z]\\b") %>%
  # Remove jr, sr, other titles
  str_remove_all("\\b(JR|SR|MR|DR|II|III|IV)\\b") %>%
  str_squish() %>%
  return()
}

make_fuzzy_match <- function(df, name_id_col, name_col) {
unique_df <- unique(df[, .(name_id = get(name_id_col), name = get(name_col))])

df_outer_join <- expand_grid(
  name_id_1 = df[, unique(get(name_id_col))],
  name_id_2 = df[, unique(get(name_id_col))],
  .name_repair = "unique"
)

setDT(df_outer_join)

df_outer_join <- df_outer_join %>%
  merge(unique_df, by.y = "name_id", by.x = "name_id_1")

df_outer_join <- df_outer_join %>%
  merge(unique_df, by.y = "name_id", by.x = "name_id_2", suffixes = c("_1", "_2"))

setDT(df_outer_join)

df_outer_join[, string_distance_jw := stringdist::stringdist(name_1, name_2, method = "jw")]

return(df_outer_join)
}

process_fuzzy_match <- function(df, name_id_col, name_col, thresh = 0.05) {
outer_join <- make_fuzzy_match(df, name_id_col, name_col)

filtered_df <- outer_join[string_distance_jw > 0 & string_distance_jw <= thresh][order(string_distance_jw)]

filtered_df[, new_id_2 := first(name_id_2), by = .(name_id_1)]
filtered_df[, new_id_1 := first(name_id_1), by = .(name_id_2)]

# Ensure unique edges (undirected graph)
dt <- filtered_df %>%
  rename(
    id1 = new_id_1,
    id2 = new_id_2
  )

edges <- unique(dt[, .(from = pmin(id1, id2), to = pmax(id1, id2))])

# Create graph from edges
g <- graph_from_data_frame(edges, directed = FALSE)

# Find connected components
components <- components(g)

# Create mapping table
mapping <- data.table(entity = names(components$membership), group_id = components$membership)

# Convert entity to numeric
mapping[, entity := as.integer(entity)]

# Merge mapping back onto filtered dataframe
filtered_df_m <- merge(filtered_df, mapping, by.x = "name_id_1", by.y = "entity")
filtered_df_m[, new_id_1 := min(new_id_1), by = .(group_id)]

xwalk <- unique(filtered_df_m[, .(name_id_1, new_id_1)]) %>%
  bind_rows(
    unique(outer_join[!name_id_1 %in% filtered_df_m$name_id_1, .(name_id_1, new_id_1 = name_id_1)])
  )

return(xwalk)
}

# ============================================================
# LOAD INPUT DATA
# ============================================================

input_path <- p_input(cfg, "evictions_party_names")
logf("Loading party names from: ", input_path, log_file = log_file)

names <- fread(input_path)
logf("  Loaded ", nrow(names), " rows, ", ncol(names), " columns", log_file = log_file)

# ============================================================
# NAME CLEANING
# ============================================================

logf("Cleaning and standardizing names...", log_file = log_file)

names[, name := str_to_upper(name)]
names[, business := str_detect(name, regex(pattern, ignore_case = TRUE))]
names[, name_clean := clean_name(name)]
names[, name_clean_no_spaces := str_remove_all(name_clean, "\\s")]
names[, name_clean_id := .GRP, by = .(name_clean, business)]
names[, name_clean_no_spaces_id := .GRP, by = .(name_clean_no_spaces, business)]
names[, short_name := fifelse(business == TRUE,
                             make_business_short_name(name_clean),
                             make_person_short_name(name_clean))]

names[, short_name := coalesce(short_name, name_clean)]
names[, short_name := fifelse(short_name == "", name_clean, short_name)]

names[, short_name_id := .GRP, by = .(short_name, business)]
names[, short_name_no_spaces := str_remove_all(short_name, "\\s")]

logf("  Unique clean names: ", names[, uniqueN(name_clean_id)], log_file = log_file)
logf("  Unique short names: ", names[, uniqueN(short_name_id)], log_file = log_file)
logf("  Business names: ", names[business == TRUE, .N], " (",
     round(100 * names[business == TRUE, .N] / nrow(names), 1), "%)", log_file = log_file)

# ============================================================
# FUZZY MATCHING
# ============================================================

logf("Performing fuzzy matching to link similar names...", log_file = log_file)

# Get all two letter combos for blocking
two_letter_combos <- expand_grid(Var1 = LETTERS, Var2 = LETTERS) %>%
mutate(two_letter_combo = str_c(Var1, Var2))

# Only need to do non-plaintiffs for defendant matching
names_unique <- unique(names[role != "Plaintiff", .(short_name_id, short_name, business)])

# Global column names
name_id_col <- "short_name_id"
name_col <- "short_name"

# Split by first two letters for blocking
letters_split <- lapply(two_letter_combos$two_letter_combo,
                       function(x) names_unique[str_detect(get(name_col), str_c("^", x))])
letters_split <- letters_split[sapply(letters_split, function(x) nrow(x) > 0)]

logf("  Processing ", length(letters_split), " letter blocks...", log_file = log_file)

# Process all two-letter combinations
xwalk_list <- lapply(letters_split, function(x) process_fuzzy_match(x, name_id_col, name_col, thresh = 0.075))

xwalk <- bind_rows(xwalk_list) %>% unique()

# Append names that didn't change
xwalk <- rbindlist(
list(
  xwalk,
  unique(names[!get(name_id_col) %in% xwalk[, name_id_1],
               .(name_id_1 = get(name_id_col), new_id_1 = get(name_id_col))])
)
)
setorder(xwalk, name_id_1)

logf("  Fuzzy match crosswalk: ", nrow(xwalk), " entries", log_file = log_file)
logf("  Names matched to different ID: ", xwalk[name_id_1 != new_id_1, .N], log_file = log_file)

# ============================================================
# MERGE CROSSWALK BACK TO NAMES
# ============================================================

logf("Merging crosswalk back to names...", log_file = log_file)

unique_names <- names[, .(
short_name = first(get(name_col)),
business = first(business)
), by = get(name_id_col)]

# Merge names back onto xwalk
xwalk_m <- merge(
xwalk,
unique_names,
by.x = "name_id_1",
by.y = "get",
all.x = TRUE
) %>%
merge(
  unique_names,
  by.x = "new_id_1",
  by.y = "get",
  suffixes = c("_original", "_new")
)

xwalk_m[, num_ids_matched := .N, by = new_id_1]
xwalk_m[, dup := .N > 1, by = name_id_1]

# Break the person matches
xwalk_m[, person_match := all(business_original == FALSE), by = new_id_1]

# Recalculate string distance between final matches
xwalk_m[, string_distance_jw := stringdist::stringdist(short_name_original,
                                                      short_name_new,
                                                      method = "jw")]

xwalk_m[, person_match_final := (business_original == FALSE & business_new == FALSE)]

# Break matches based on thresholds
xwalk_m[, break_match := ((string_distance_jw > 0.05 & person_match_final == TRUE) |
                          (string_distance_jw > 0.15 & person_match_final == FALSE) |
                          num_ids_matched > 100)]

xwalk_m[, short_name_new := fifelse(break_match == TRUE, short_name_original, short_name_new)]

logf("  Matches broken due to thresholds: ", xwalk_m[break_match == TRUE, .N], log_file = log_file)

# Merge back onto names
names_merge <- merge(
names,
xwalk_m,
by.x = name_id_col,
by.y = "name_id_1",
all.x = TRUE
)

names_merge[, commercial_case_og := any(business[role != "Plaintiff"]), by = id]
names_merge[, commercial_case_new := any(business_new[role != "Plaintiff"] | business[role != "Plaintiff"]), by = id]

logf("  Commercial cases (original): ", names_merge[, uniqueN(id[commercial_case_og == TRUE])], log_file = log_file)
logf("  Commercial cases (after matching): ", names_merge[, uniqueN(id[commercial_case_new == TRUE])], log_file = log_file)

# ============================================================
# ASSERTIONS
# ============================================================

logf("Running assertions...", log_file = log_file)

# Check required columns exist
required_cols <- c("id", "role", "name", "name_clean", "short_name", "business")
missing_cols <- setdiff(required_cols, names(names_merge))
if (length(missing_cols) > 0) {
stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
}
logf("  Required columns: PASS", log_file = log_file)

# Check no rows were lost
if (nrow(names_merge) < nrow(names)) {
stop("Rows lost during merge: ", nrow(names), " -> ", nrow(names_merge))
}
logf("  Row count preserved: PASS (", nrow(names_merge), " rows)", log_file = log_file)

# ============================================================
# WRITE OUTPUT
# ============================================================

out_path <- p_product(cfg, "eviction_names_clean")
logf("Writing output to: ", out_path, log_file = log_file)

# Ensure output directory exists
dir.create(dirname(out_path), showWarnings = FALSE, recursive = TRUE)

fwrite(names_merge, out_path)
logf("  Wrote ", nrow(names_merge), " rows, ", ncol(names_merge), " columns", log_file = log_file)

logf("=== Finished clean-eviction-names.R ===", log_file = log_file)
