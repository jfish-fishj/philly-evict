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



