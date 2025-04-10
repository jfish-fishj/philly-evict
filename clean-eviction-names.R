library(data.table)
library(tidyverse)
library(sf)
library(tidycensus)
library(stringr)

names <- fread("~/Desktop/data/philly-evict/phila-lt-data/party-names-addresses.txt")

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
  "HOUS","HOUSING","APARTMENTS","APTS?","REAL",
  "ESTATE","REALTY","MANAGEMENT","MGMT",
  "DEV","DEVELOPMENT","DEVELOPS","DEVELOPERS",
  "THE"
)

# Standardization function
# Create regex pattern (case insensitive)
pattern <- str_c("\\b(", str_c(business_words, collapse = "|"), ")\\b", collapse = "|")


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
      str_detect(.,"(PHILA.+HOU\\s?S|HOUS.+ AUTH?|PHILA.+AUTH|ADELPHIA HOUSE)"),
      "PHILADELPHIA HOUSING AUTHORITY", .) %>%
    str_squish() %>%
    str_trim() %>%
    return()
}

clean_name <- function(name){
  name %>%
    # replace & with AND
    str_replace_all("([^\\s])&([^\\s])", "\\1 AND \\2") %>%
    str_replace_all("([^\\s]),([^\\s])", "\\1 , \\2") %>%
    str_replace_all("&", "AND") %>%
    # strip punctuation
    str_replace_all("[[:punct:]]"," ") %>%
    str_squish() %>%
    str_remove_all("((AND)?\\s?(ALL OTHER OCCUPANTS?|ALL OTHERS?|ALL OCCUPANTS?))") %>%
    str_remove_all("((AND)?\\s?(ALL OCCSUPS?|ALL OCCUPS?))") %>%
    str_remove_all("((AND)?\\s?(ALL OCCS?|ALL OTHER OCCS?))") %>%
    fix_spellings() %>%
    str_squish() %>%
    return()
}

make_business_short_name <- function(x){
  x %>%
    # remove business words or middle initials
    str_remove_all(regex(pattern, ignore_case = TRUE)) %>%
    # remove leading "the"
    str_remove("^THE\\s") %>%
    # remove trailing roman numerals
    str_remove("\\b(VI{0,3}|IV|IX|XI{0,3}|II|I|III)[\\b$]") %>%
    str_remove("\\b([0-9+])$") %>%
    str_squish() %>%
    return()
}

make_person_short_name <- function(x){
  x %>%
  # remove middle initials, e.g, joe d fish -> joe fish
  str_remove_all("\\b[A-Z]\\b") %>%
    # remove jr, sr, other titles
    str_remove_all("\\b(JR|SR|MR|DR|II|III|IV)\\b") %>%
    str_squish() %>%
  return()
}


# Remove business words using stringr


library(data.table)
library(dplyr)
library(glue)
library(stringdist)
library(igraph)

make_fuzzy_match <- function(df, name_id_col, name_col) {
  #print(glue("Number of unique {name_id_col} values: {length(unique(df[[name_id_col]]))}"))

  unique_df <- unique(df[, .(name_id = get(name_id_col), name = get(name_col))])

  df_outer_join <- expand_grid(
    name_id_1 = df[, unique(get(name_id_col))],
    name_id_2 = df[, unique(get(name_id_col))],
    .name_repair = "unique"
  )

  #print(glue("Number of rows in outer join: {nrow(df_outer_join)}"))
  setDT(df_outer_join)

  df_outer_join <- df_outer_join %>%
    merge(unique_df, by.y = "name_id", by.x = "name_id_1")

  #print(glue("Number of rows after first merge: {nrow(df_outer_join)}"))

  df_outer_join <- df_outer_join %>%
    merge(unique_df, by.y = "name_id", by.x = "name_id_2", suffixes = c("_1", "_2"))

  setDT(df_outer_join)
  #print(glue("Number of rows after second merge: {nrow(df_outer_join)}"))

  df_outer_join[, string_distance_jw := stringdist::stringdist(name_1, name_2, method = "jw")]

  return(df_outer_join)
}

process_fuzzy_match <- function(df, name_id_col, name_col, thresh = 0.05) {
  outer_join <- make_fuzzy_match(df, name_id_col, name_col)
  initial_rows <- nrow(outer_join)

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


# do names but constrain to match on first letter




#View(names_sample %>% select(contains("name")))

names[,name := str_to_upper(name)]
names[,business := str_detect(name, regex(pattern, ignore_case = TRUE))]
names[,name_clean :=clean_name(name)]
names[,name_clean_no_spaces := str_remove_all(name_clean, "\\s")]
names[,name_clean_id := .GRP, by = .(name_clean,business)]
names[,name_clean_no_spaces_id := .GRP, by = .(name_clean_no_spaces,business)]
names[, short_name := fifelse(business == T,
                                     make_business_short_name(name_clean),
                                     make_person_short_name(name_clean))]

names[,short_name := coalesce(short_name, name_clean)]
names[,short_name := fifelse(short_name == "", name_clean, short_name)]

names[,short_name_id := .GRP, by = .(short_name,business)]
names[,short_name_no_spaces := str_remove_all(short_name, "\\s")]

# get all two letter combos
two_letter_combos <- expand_grid(Var1 = LETTERS, Var2 = LETTERS) %>%
  mutate(two_letter_combo = str_c(Var1, Var2))

# only need to do plaintiffs
names_plaintiff = names[role == "Plaintiff"]

names_unique = unique(names[role != "Plaintiff", .(short_name_id, short_name, business)])

# Global column names
name_id_col <- "short_name_id"
name_col <- "short_name"


letters_split <- lapply(two_letter_combos$two_letter_combo, function(x) names_unique[str_detect(get(name_col), str_c("^",x))])
sapply(letters_split, function(x) nrow(x) ) %>% quantile()

sapply(letters_split, function(x) x[,uniqueN(short_name)] ) %>% quantile()
letters_split = letters_split[sapply(letters_split, function(x) nrow(x) > 0)]


# Process all two-letter combinations
xwalk_list <- lapply(letters_split, function(x) process_fuzzy_match(x, name_id_col, name_col, thresh = 0.075))

xwalk <- bind_rows(xwalk_list) %>% unique()

# Append names that didn't change
xwalk <- rbindlist(
  list(
    xwalk,
    unique(names[!get(name_id_col) %in% xwalk[,name_id_1],
                           .(name_id_1 = get(name_id_col), new_id_1 = get(name_id_col))])
  )
)
setorder(xwalk, name_id_1)

unique_names <- names[, .(
  short_name = first(get(name_col)),
  business = first(business)
), by = get(name_id_col)]

# Merge names back onto xwalk
xwalk_m <- merge(
  xwalk,
  unique_names,
  by.x = "name_id_1",
  by.y = 'get',
  all.x = TRUE
) %>%
  merge(
    unique_names,
    by.x = "new_id_1",
    by.y = 'get',
    suffixes = c("_original", "_new")
  )

xwalk_m[, num_ids_matched := .N, by = new_id_1]
xwalk_m[, dup := .N > 1, by = name_id_1]

# Break the person matches
xwalk_m[, person_match := all(business_original == FALSE), by = new_id_1]

# Recalculate string distance between final matches
xwalk_m[, string_distance_jw := stringdist::stringdist((short_name_original),
                                                       (short_name_new),
                                                       method = "jw")]

xwalk_m[,person_match_final := (business_original == F & business_new == F)]

# Break matches based on thresholds
xwalk_m[, break_match := ((string_distance_jw > 0.05 & person_match_final == TRUE) |
                            (string_distance_jw > 0.15 & person_match_final == FALSE) |
                            num_ids_matched > 100)]

xwalk_m[,short_name_new := fifelse(break_match == T, short_name_original, short_name_new)]

print(xwalk[,.N, by = name_id_1!=new_id_1])
# take breaks and redo matches with lower threshold
#names_breaks = names_plaintiff[short_name_id %in% xwalk_m[break_match == T,short_name_id_1]]
#View(xwalk_m[name_id_1!=new_id_1])

# merge back onto names plaintiff
names_merge = merge(
  names,
  xwalk_m,
  by.x = name_id_col,
  by.y = "name_id_1",
  all.x = TRUE
)

names_merge[,commercial_case_og := any(business[role != "Plaintiff"]), by = id]
names_merge[,commercial_case_new := any(business_new[role != "Plaintiff"]|business[role != "Plaintiff"]), by = id]
names_merge[,uniqueN(id), by = .(commercial_case_og, commercial_case_new)]

fwrite(names_merge, "~/Desktop/data/philly-evict/phila-lt-data/names_merge_defendants.csv")

# View(names_plaintiff_merge[,.N, by = short_name_original])
# View(names_plaintiff_merge[,.N, by = short_name_new])

# names_breaks_split = lapply(two_letter_combos$two_letter_combo, function(x) names_breaks[str_detect(name_clean, str_c("^",x))])
# names_breaks_split = names_breaks_split[sapply(names_breaks_split, function(x) nrow(x) > 0)]
# xwalk_list_breaks = lapply(names_breaks_split, function(x) process_fuzzy_match(x, thresh = 0.05))
# xwalk_breaks = bind_rows(xwalk_list_breaks)
# xwalk_breaks = rbindlist(list(
#   xwalk_breaks,
#   unique(names_breaks[!short_name_id %in% unlist(xwalk_list_breaks),
#                .(short_name_id_1=short_name_id, new_id_1=short_name_id)])
# ))
# setorder(xwalk_breaks,short_name_id_1)
# unique_names_breaks = (names_breaks[,list(
#   short_name=first(short_name),
#   business = first(business)
# ), by =short_name_id
# ] )
#
# # merge back on names onto xwalk
# xwalk_m_breaks = merge(
#   xwalk_breaks,
#   unique_names_breaks,
#   by.x = "short_name_id_1",
#   by.y = "short_name_id",
#   all.x=T
# ) %>%
#   merge(
#     unique_names_breaks,
#     by.x = "new_id_1",by.y = "short_name_id",
#     suffixes = c("_original", "_new")
# )
#
# xwalk_m_breaks[,num_ids_matched := .N, by = new_id_1]
#
# # break the person matches
# xwalk_m_breaks[,person_match := all(business_original == F), by = new_id_1]
#
# # redo string distance between final matches
# xwalk_m_breaks[,string_distance_jw := stringdist::stringdist((short_name_original),
#                                                       (short_name_new),
#                                                       method = "jw")]
#
# # break matches
# xwalk_m_breaks[,break_match := ((string_distance_jw > 0.05 & person_match == T) | (string_distance_jw > 0.15 & person_match == F) |num_ids_matched > 100)]






