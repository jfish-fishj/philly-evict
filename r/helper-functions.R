library(tidyverse)
library(data.table)
library(janitor)
#library(sf)
library(spatstat)

theme_philly_evict <- function(){
  theme_bw() +
    theme(legend.position = 'right',
          legend.direction = "vertical",
          strip.text = element_text(colour = "black",size=25),
          strip.background =element_rect(fill="white"),
          plot.title = element_text(size=22, face = "bold"),
          plot.subtitle  = element_text(size=18, face = "bold"),
          axis.title.x=element_text(size=20, margin = margin(t = 20)),
          axis.title.y=element_text(size=20),
          legend.title = element_text(size=20, color = "blue"),
          plot.caption = element_text(hjust = 0)
    ) %>%
    return()
}


#' Compute empirical cumulative distribution
#'
#' The empirical cumulative distribution function (ECDF) provides an alternative
#' visualisation of distribution. Compared to other visualisations that rely on
#' density (like [geom_histogram()]), the ECDF doesn't require any
#' tuning parameters and handles both continuous and categorical variables.
#' The downside is that it requires more training to accurately interpret,
#' and the underlying visual tasks are somewhat more challenging.
#'
#' @inheritParams layer
#' @inheritParams geom_point
#' @param na.rm If `FALSE` (the default), removes missing values with
#'    a warning.  If `TRUE` silently removes missing values.
#' @param n if NULL, do not interpolate. If not NULL, this is the number
#'   of points to interpolate with.
#' @param pad If `TRUE`, pad the ecdf with additional points (-Inf, 0)
#'   and (Inf, 1)
#' @section Computed variables:
#' \describe{
#'   \item{x}{x in data}
#'   \item{y}{cumulative density corresponding x}
#' }
#' @export
#' @examples
#' df <- data.frame(
#'   x = c(rnorm(100, 0, 3), rnorm(100, 0, 10)),
#'   g = gl(2, 100)
#' )
#' ggplot(df, aes(x)) + stat_ecdf(geom = "step")
#'
#' # Don't go to positive/negative infinity
#' ggplot(df, aes(x)) + stat_ecdf(geom = "step", pad = FALSE)
#'
#' # Multiple ECDFs
#' ggplot(df, aes(x, colour = g)) + stat_ecdf()
stat_ecdf <- function(mapping = NULL, data = NULL,
                      geom = "step", position = "identity",
                      weight =  NULL,
                      ...,
                      n = NULL,
                      pad = TRUE,
                      na.rm = FALSE,
                      show.legend = NA,
                      inherit.aes = TRUE) {
  layer(
    data = data,
    mapping = mapping,
    stat = StatEcdf,
    geom = geom,
    position = position,
    show.legend = show.legend,
    inherit.aes = inherit.aes,
    params = list(
      n = n,
      pad = pad,
      na.rm = na.rm,
      weight = weight,
      ...
    )
  )
}


#' @rdname ggplot2-ggproto
#' @format NULL
#' @usage NULL
#' @export
#'

StatEcdf <- ggproto("StatEcdf", Stat,
                    compute_group = function(data, scales, weight, n = NULL, pad = TRUE) {
                      # If n is NULL, use raw values; otherwise interpolate
                      if (is.null(n)) {
                        x <- unique(data$x)
                      } else {
                        x <- seq(min(data$x), max(data$x), length.out = n)
                      }

                      if (pad) {
                        x <- c(-Inf, x, Inf)
                      }
                      y <- ewcdf(data$x, weights=data$weight/sum(data$weight))(x)

                      data.frame(x = x, y = y)
                    },

                    default_aes = aes(y = stat(y)),

                    required_aes = c("x")
)

Mode <- function(x, na.rm = FALSE) {
  if(na.rm){
    x = x[!is.na(x)]
  }

  ux <- unique(x)
  return(ux[which.max(tabulate(match(x, ux)))])
}

impute_units <- function(col){
  col = col %>%
    str_remove("\\.0+")
  case_when(
    str_detect(col, regex("studio", ignore_case = T)) ~ 0,
    str_detect(col, regex("(zero|0)[\\s,]?(bd|bed|br[^a-z])", ignore_case = T)) ~ 0,
    str_detect(col, regex("(one|1)[\\s,]?(bd|bed|br[^a-z])", ignore_case = T)) ~ 1,
    str_detect(col, regex("(two|2)[\\s,]?(bd|bed|br[^a-z])", ignore_case = T)) ~ 2,
    str_detect(col, regex("(three|3)[\\s,]?(bd|bed|br[^a-z])", ignore_case = T)) ~ 3,
    str_detect(col, regex("(four|4)[\\s,]?(bd|bed|br[^a-z])", ignore_case = T)) ~ 4,
    str_detect(col, regex("(five|5)[\\s,]?(bd|bed|br[^a-z])", ignore_case = T)) ~ 5,
    TRUE ~ NA_real_

  ) %>% return()
}

impute_baths <- function(col){
  col = col %>%
    str_remove("\\.0+")
  case_when(
    str_detect(col, regex("(zero|0)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 0,
    str_detect(col, regex("(0.5)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 0.5,
    str_detect(col, regex("(one|1)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 1,
    str_detect(col, regex("(1.5)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 1,
    str_detect(col, regex("(two|2)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 2,
    str_detect(col, regex("(2.5)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 2.5,
    str_detect(col, regex("(three|3)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 3,
    str_detect(col, regex("(four|4)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 4,
    str_detect(col, regex("(five|5)[\\s,]?(bath|ba[^a-z])", ignore_case = T)) ~ 5,
    TRUE ~ NA_real_

  ) %>% return()
}


cities_fips <- tibble(
  city = c(
    "austin",
    #"baltimore",
    #"boston",
    "chicago", "cleveland",
    "dallas", "detroit", "houston", "los_angeles", "miami",
    "philadelphia"
  ),
  county = c(
    "Travis County, TX",       # Austin
   # "Baltimore city, MD",      # Independent city
   # "Suffolk County, MA",      # Boston
    "Cook County, IL",         # Chicago
    "Cuyahoga County, OH",     # Cleveland
    "Dallas County, TX",       # Dallas
    "Wayne County, MI",        # Detroit
    "Harris County, TX",       # Houston
    "Los Angeles County, CA",  # Los Angeles
    "Miami-Dade County, FL",   # Miami
    "Philadelphia County, PA"  # Philadelphia
  ),
  fips = c(
    "48453",  # Travis County, TX
   # "24510",  # Baltimore city, MD
   # "25025",  # Suffolk County, MA
    "17031",  # Cook County, IL
    "39035",  # Cuyahoga County, OH
    "48113",  # Dallas County, TX
    "26163",  # Wayne County, MI
    "48201",  # Harris County, TX
    "06037",  # Los Angeles County, CA
    "12086",  # Miami-Dade County, FL
    "42101"   # Philadelphia County, PA
  )
)

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
  "RESTAURANT","RESTARAUNT","ACADEMY",
  "DEV","DEVELOPMENT","DEVELOPS","DEVELOPERS",
  "THE"
)

# Standardization function
# Create regex pattern (case insensitive)
business_regex <- regex(str_c("\\b(", str_c(business_words, collapse = "|"), ")\\b", collapse = "|"), ignore_case = T)


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
    str_replace_all("|", " | ") %>%
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


# =============================================================================
# BUILDING CODE STANDARDIZATION FUNCTIONS
# =============================================================================

#' Extract number of stories from building code description
#'
#' Parses building code descriptions to extract explicit story counts.
#' Handles patterns like "2 STY", "3.5 STY", "10-14 STY" (returns midpoint for ranges).
#'
#' @param bldg_code_desc Character vector of building code descriptions
#'   (typically from building_code_description column)
#' @return Numeric vector of story counts (NA if not found)
#'
#' @examples
#' extract_stories_from_code("ROW 2 STY MASONRY")
#' # Returns: 2
#' extract_stories_from_code("APT 2-4 UNITS 3 STY MASON")
#' # Returns: 3
#' extract_stories_from_code("APTS 5-50 UNITS MASONRY")
#' # Returns: NA (no stories mentioned)
#' extract_stories_from_code("PUB UTIL 10-14 STY MASON")
#' # Returns: 12 (midpoint of range)
extract_stories_from_code <- function(bldg_code_desc) {
  # Handle NA/empty inputs
  result <- rep(NA_real_, length(bldg_code_desc))
  valid_idx <- !is.na(bldg_code_desc) & nchar(bldg_code_desc) > 0

  if (!any(valid_idx)) return(result)

  desc <- toupper(bldg_code_desc[valid_idx])


  # Pattern 1: Range like "10-14 STY" - take midpoint
  range_pattern <- "\\b(\\d+)-(\\d+)\\s*STY"
  range_matches <- str_match(desc, range_pattern)
  has_range <- !is.na(range_matches[, 1])

  # Pattern 2: Single number like "2 STY" or "3.5 STY"
  single_pattern <- "\\b(\\d+\\.?\\d*)\\s*STY"
  single_matches <- str_match(desc, single_pattern)
  has_single <- !is.na(single_matches[, 1]) & !has_range

  # Extract values
  stories <- rep(NA_real_, length(desc))

  # Range: take midpoint

  if (any(has_range)) {
    low <- as.numeric(range_matches[has_range, 2])
    high <- as.numeric(range_matches[has_range, 3])
    stories[has_range] <- (low + high) / 2
  }

  # Single value
  if (any(has_single)) {
    stories[has_single] <- as.numeric(single_matches[has_single, 2])
  }

  result[valid_idx] <- stories
  return(result)
}


#' Standardize building type from Philadelphia building code descriptions
#'
#' Classifies parcels into standardized building types using both the old and new
#' building code description columns, plus optional context from building footprint
#' data. Designed to distinguish between high-rise apartments and multi-building
#' garden-style complexes.
#'
#' @param bldg_code_desc Character vector - the "old" building code description
#'   (e.g., "ROW 2 STY MASONRY", "APT 2-4 UNITS 3 STY MASON")
#' @param bldg_code_desc_new Character vector - the "new" building code description
#'   (e.g., "ROW TYPICAL", "APARTMENTS - LOW RISE")
#' @param num_bldgs Numeric vector (optional) - number of buildings on parcel,
#'   from building_pid_xwalk. Used to identify multi-building complexes.
#' @param num_stories Numeric vector (optional) - number of stories, either from
#'   extract_stories_from_code() or from parcel data. Used to distinguish rise types.
#'
#' @return A list with two elements:
#'   \itemize{
#'     \item building_type: Character vector with size-based building types:
#'       \itemize{
#'         \item "DETACHED" - single-family detached (DET patterns)
#'         \item "ROW" - rowhouse/attached single-family
#'         \item "TWIN" - semi-detached/duplex (SEMI/DET, S/D patterns)
#'         \item "SMALL_MULTI_2_4" - 2-4 unit building
#'         \item "LOWRISE_MULTI" - 5-19 units, typically <=3 stories
#'         \item "MIDRISE_MULTI" - 20-49 units or 4-6 stories
#'         \item "HIGHRISE_MULTI" - 50+ units or 7+ stories, single building
#'         \item "MULTI_BLDG_COMPLEX" - multiple buildings on parcel (garden-style)
#'         \item "COMMERCIAL" - commercial properties (both columns agree, or standalone hotel)
#'         \item "GROUP_QUARTERS" - boarding houses, dormitories, rooming houses
#'         \item "OTHER" - unable to classify
#'       }
#'     \item is_condo: Logical vector indicating if parcel is a condo
#'   }
#'
#' @examples
#' result <- standardize_building_type("ROW 2 STY MASONRY", "ROW TYPICAL")
#' result$building_type  # "ROW"
#' result$is_condo       # FALSE
#'
#' result <- standardize_building_type("RES CONDO 5 STY MASONRY", "CONDO")
#' result$building_type  # "MIDRISE_MULTI" (classified by size)
#' result$is_condo       # TRUE
standardize_building_type <- function(bldg_code_desc,
                                       bldg_code_desc_new,
                                       num_bldgs = NA_integer_,
                                       num_stories = NA_real_) {

  n <- length(bldg_code_desc)

  # Ensure all vectors are same length
  if (length(bldg_code_desc_new) == 1) bldg_code_desc_new <- rep(bldg_code_desc_new, n)
  if (length(num_bldgs) == 1) num_bldgs <- rep(num_bldgs, n)
  if (length(num_stories) == 1) num_stories <- rep(num_stories, n)

  # Uppercase for matching
  old_desc <- toupper(coalesce(as.character(bldg_code_desc), ""))
  new_desc <- toupper(coalesce(as.character(bldg_code_desc_new), ""))

  # Initialize results
  result <- rep(NA_character_, n)
  is_condo <- rep(FALSE, n)

  # -------------------------------------------------------------------------
  # Detect condos (separate flag, not a building type)
  # -------------------------------------------------------------------------
  is_condo <- grepl("CONDO", old_desc) | grepl("CONDO", new_desc)

  # -------------------------------------------------------------------------
  # Detect commercial - BOTH columns must indicate commercial
  # This prevents hotels, office buildings from being classified as residential
  # -------------------------------------------------------------------------
  commercial_patterns_old <- "HOTEL|OFFICE|OFF\\s*BLD|BANK|STORE\\b|STR/OFF|WAREHOUSE|" %+%
                             "FACTORY|IND\\s*BLDG|IND\\s*SHOP|IND\\s*WAREHOUSE|" %+%
                             "COMMERCIAL|RETAIL|TAVERN|REST.RNT|AUTO\\s*REPAIR|" %+%
                             "AUTO\\s*DEALER|PARKING|PKG\\s*LOT|SCHOOL|HOSPITAL|" %+%
                             "HEALTH\\s*FAC|HSE\\s*WORSHIP|CHURCH|CEMETERY|" %+%
                             "AMUSE|MISC\\s*ADMIN|PUB\\s*UTIL"
  commercial_patterns_new <- "RETAIL|WAREHOUSE|OFFICE|HOTEL|HOSPITAL|SCHOOL|" %+%
                             "RELIGIOUS|COMMERCIAL|INDUSTRIAL|MANUFACTURING|" %+%
                             "DOWNTOWN\\s*ROW|MEDICAL|CULTURAL|PARKING|" %+%
                             "RECREATIONAL|COUNTRY\\s*CLUB|DAY\\s*CARE|" %+%
                             "GOV.T|COLLEGES|THEATER|GYMNASIUM"

  is_commercial_old <- grepl(commercial_patterns_old, old_desc)
  is_commercial_new <- grepl(commercial_patterns_new, new_desc)

  # Only flag as commercial if BOTH columns agree (conservative approach)
  is_commercial_both <- is_commercial_old & is_commercial_new

  # Also flag as commercial if old column is clearly non-residential and new is empty/generic
  is_clearly_commercial <- grepl("^(HOTEL|OFFICE|OFF\\s*BLD|BANK|WAREHOUSE|FACTORY|" %+%
                                  "IND\\s*BLDG|IND\\s*SHOP|SCHOOL|HOSPITAL|" %+%
                                  "HSE\\s*WORSHIP|CEMETERY|PUB\\s*UTIL)", old_desc) &
                           !grepl("APT|APTS|APARTMENT|RESID|DWELLING", old_desc)

  # Standalone hotel detection: if EITHER column says HOTEL and neither says

  # APT/APARTMENT/RESID, classify as COMMERCIAL. The "both columns agree" rule
  # above misses hotels with only one column populated.
  is_hotel_either <- (grepl("HOTEL", old_desc) | grepl("HOTEL", new_desc)) &
                     !grepl("APT|APARTMENT|RESID", old_desc) &
                     !grepl("APT|APARTMENT|RESID", new_desc)

  is_commercial <- is_commercial_both | is_clearly_commercial | is_hotel_either
  result[is_commercial] <- "COMMERCIAL"

  # -------------------------------------------------------------------------
  # Priority 1: Multi-building complexes (garden-style)
  # If num_bldgs > 3, it's a complex regardless of total units
  # Skip if already classified as commercial
  # -------------------------------------------------------------------------
  is_complex <- !is.na(num_bldgs) & num_bldgs > 3
  result[is_complex & is.na(result)] <- "MULTI_BLDG_COMPLEX"

  # -------------------------------------------------------------------------
  # Priority 2: Explicit apartment size in NEW column
  # Only apply to residential buildings
  # -------------------------------------------------------------------------
  is_highrise_new <- grepl("HIGH\\s*RISE|APTS?\\s*-?\\s*HIGH", new_desc) |
                     grepl("TOWER", new_desc)
  is_midrise_new  <- grepl("MID\\s*RISE|APTS?\\s*-?\\s*MID", new_desc)
  is_lowrise_new  <- grepl("LOW\\s*RISE|APTS?\\s*-?\\s*LOW", new_desc)
  is_garden_new   <- grepl("GARDEN", new_desc)

  # Must also have residential indicator to use these
 is_residential_new <- grepl("APARTMENT|RESID|BLT\\s*AS\\s*RES", new_desc)

  result[is_highrise_new & is_residential_new & is.na(result)] <- "HIGHRISE_MULTI"
  result[is_midrise_new & is_residential_new & is.na(result)]  <- "MIDRISE_MULTI"
  result[is_lowrise_new & is_residential_new & is.na(result)]  <- "LOWRISE_MULTI"
  result[is_garden_new & is_residential_new & is.na(result)]   <- "LOWRISE_MULTI"

  # -------------------------------------------------------------------------
  # Priority 3: Unit count patterns in OLD column (residential apartments)
  # -------------------------------------------------------------------------
  # APTS 100+ or APTS 51-100 -> large multi
  is_100plus <- grepl("APTS?\\s*(100\\+|100-|101)", old_desc)
  is_51_100  <- grepl("APTS?\\s*51-?100", old_desc)
  # APTS 5-50 -> could be mid or low rise depending on stories
  is_5_50    <- grepl("APTS?\\s*5-?50", old_desc)
  # APT 2-4 -> small multi
  is_2_4     <- grepl("APT\\s*2-?4", old_desc)

  # 100+ units: highrise unless multi-building
  result[is_100plus & is.na(result)] <- "HIGHRISE_MULTI"
  # 51-100 units: highrise unless multi-building or low stories
  result[is_51_100 & is.na(result)] <- "HIGHRISE_MULTI"

  # 5-50 units: depends on stories
  is_5_50_unassigned <- is_5_50 & is.na(result)
  result[is_5_50_unassigned & !is.na(num_stories) & num_stories >= 6] <- "HIGHRISE_MULTI"
  result[is_5_50_unassigned & !is.na(num_stories) & num_stories >= 4 & num_stories < 6] <- "MIDRISE_MULTI"
  result[is_5_50_unassigned & !is.na(num_stories) & num_stories < 4] <- "LOWRISE_MULTI"
  # If no story info, default to lowrise for 5-50
  result[is_5_50 & is.na(result)] <- "LOWRISE_MULTI"

  # 2-4 units
  result[is_2_4 & is.na(result)] <- "SMALL_MULTI_2_4"

  # -------------------------------------------------------------------------
  # Priority 4: Story-based classification for remaining unclassified
  # Only if there's a residential indicator
  # -------------------------------------------------------------------------
  has_stories <- !is.na(num_stories)
  is_residential_old <- grepl("APT|APTS|DWELLING|RESID|RES\\s*CONDO|STACKED\\s*PUD", old_desc)

  # 7+ stories with residential indicator -> highrise
 result[has_stories & num_stories >= 7 & is_residential_old & is.na(result)] <- "HIGHRISE_MULTI"
  # 4-6 stories with residential indicator -> midrise
  result[has_stories & num_stories >= 4 & num_stories < 7 & is_residential_old & is.na(result)] <- "MIDRISE_MULTI"

  # -------------------------------------------------------------------------
  # Priority 5: Structural type patterns (ROW, TWIN, DET)
  # -------------------------------------------------------------------------
  # Detached single-family: DET but NOT "SEMI/DET" or "S/D"
  is_detached <- (grepl("^DET\\b|\\bDET\\s", old_desc) &
                  !grepl("SEMI|S/D|APT|APTS", old_desc))
  result[is_detached & is.na(result)] <- "DETACHED"

  # Twin/Semi-detached: SEMI/DET or S/D patterns
  is_twin <- grepl("SEMI/?DET|S/D\\b|TWIN", old_desc) |
             grepl("TWIN", new_desc)
  result[is_twin & is.na(result)] <- "TWIN"

  # Row homes
  is_row <- grepl("^ROW\\b|\\bROW\\s", old_desc) |
            grepl("^ROW\\b", new_desc)
  # Exclude converted apartments
  is_row_conv_apt <- grepl("ROW\\s*CONV/?APT", old_desc)
  result[is_row & !is_row_conv_apt & is.na(result)] <- "ROW"

  # Row converted to apartments -> small multi
  result[is_row_conv_apt & is.na(result)] <- "SMALL_MULTI_2_4"

  # -------------------------------------------------------------------------
  # Priority 6a: Group quarters — BOARDING/DORMITORY/ROOMING
  # These are not commercial (they house people) but not standard residential.
  # In make-occupancy-vars.r, GROUP_QUARTERS is excluded from is_def_res but

  # can enter via is_ambig if the parcel has rental evidence.
  # -------------------------------------------------------------------------
  is_group_quarters <- grepl("BOARDING|DORMITORY|ROOMING", old_desc) &
                       !grepl("APT|APTS|APARTMENT|RESID", old_desc)
  result[is_group_quarters & is.na(result)] <- "GROUP_QUARTERS"

  # -------------------------------------------------------------------------
  # Priority 6b: Catch remaining apartment patterns
  # -------------------------------------------------------------------------
  is_apt_other <- grepl("APT|APTS|APARTMENT", old_desc) |
                  grepl("APARTMENT", new_desc)
  result[is_apt_other & is.na(result)] <- "LOWRISE_MULTI"

  # -------------------------------------------------------------------------
  # Default: OTHER
  # -------------------------------------------------------------------------
  result[is.na(result)] <- "OTHER"

  return(list(
    building_type = result,
    is_condo = is_condo
  ))
}

# Helper for string concatenation (avoids paste0 verbosity)
`%+%` <- function(a, b) paste0(a, b)

# Empirical Bayes Poisson-Gamma shrinkage for rates:
# y_i ~ Poisson(exposure_i * lambda_i), lambda_i ~ Gamma(alpha, beta)
# Posterior mean: (alpha + y_i) / (beta + exposure_i)
eb_shrink_poisson_gamma <- function(y, exposure, prior_year = NULL, year = NULL) {
  stopifnot(length(y) == length(exposure))
  y <- as.numeric(y)
  exposure <- pmax(as.numeric(exposure), 0)

  ok <- is.finite(y) & is.finite(exposure) & exposure > 0
  if (!is.null(prior_year) && !is.null(year)) {
    ok <- ok & (year <= prior_year)
  }
  if (!any(ok)) return(list(rate_eb = rep(NA_real_, length(y)), alpha = NA_real_, beta = NA_real_))

  yy <- y[ok]
  ee <- exposure[ok]

  S1 <- sum(yy, na.rm = TRUE)
  E1 <- sum(ee, na.rm = TRUE)
  mu <- if (E1 > 0) S1 / E1 else 0

  yy1 <- sum(yy * pmax(yy - 1, 0), na.rm = TRUE)
  E2  <- sum(ee^2, na.rm = TRUE)
  T   <- if (E2 > 0) yy1 / E2 else NA_real_

  eps <- 1e-12
  beta_hat <- if (!is.na(T) && (T > mu^2 + eps) && mu > 0) mu / (T - mu^2) else 1e8
  alpha_hat <- mu * beta_hat

  rate_eb <- rep(NA_real_, length(y))
  ok2 <- is.finite(y) & is.finite(exposure) & exposure > 0
  rate_eb[ok2] <- (alpha_hat + y[ok2]) / (beta_hat + exposure[ok2])

  list(rate_eb = rate_eb, alpha = alpha_hat, beta = beta_hat, mu = mu)
}
