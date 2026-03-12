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

normalize_owner_name <- function(x) {
  x |>
    as.character() |>
    stringr::str_to_upper() |>
    stringr::str_squish()
}

pha_owner_regex <- function() {
  stringr::regex(
    paste0(
      "PHILADELPHIA\\s+HOUSING",
      "|PHILA[.\\s]?HOUS[A-Z\\s.]{0,20}AUTH",
      "|\\bPAPMC\\b",
      "|AFFILIATE\\s+OF\\s+THE\\s+PHILA",
      "|\\bHOUS\\w*\\s+AUTH"
    ),
    ignore_case = TRUE
  )
}

university_owner_regex <- function() {
  positive_patterns <- c(
    "\\bDREXEL\\b",
    "THOMAS\\s+JEFFERSON",
    "JEFFERSON\\s+UNIV",
    "UNIVERSITY\\s+OF\\s+PENNSYLVANIA",
    "TRUSTEES\\s+OF\\s+(THE\\s+)?UNIV(ERSITY)?\\s+OF\\s+PENN",
    "TRS\\s+(OF\\s+THE\\s+)?UNIV\\s+OF\\s+PENN",
    "TR\\s+UNIV\\s+OF\\s+PENNA",
    "UNI\\s+PENN\\s+HOUSING",
    "\\bUPENN\\b",
    "TEMPLE\\s+UNIVERSITY",
    "TEMPLE\\s+UNIV",
    "TEMPLE\\s+UNIVERSITY\\s+HOSPIT",
    "TEMPLE\\s+(RESEV|PROPERT|NEST|VILLA|CASA|WEST|SOUTH|LOFT|ESTATE|OWL\\s+RENTALS|AREA\\s+STUDENT)",
    "ARCH\\s+[IVX]+\\s*-?\\s*TEMPLE",
    "(IFP|IRP)\\s+FUND\\s+II\\s+TEMPLE",
    "NEST\\s+AT\\s+TEMPLE",
    "LA\\s+SALLE\\s+(UNIV|COLLEGE)",
    "LASALLE\\s+(UNIV|COLLEGE|HOUSING)",
    "SAINT\\s+JOSEPH'?S?\\s+(UNIV|COLLEGE)",
    "ST[.]?\\s+JOSEPH'?S?\\s+(UNIV|COLLEGE)",
    "COMMUNITY\\s+COLLEGE\\s+OF\\s+PHILADELPHIA",
    "HOLY\\s+FAMILY\\s+(UNIV|COLLEGE)",
    "CURTIS\\s+INSTITUTE",
    "MOORE\\s+COLLEGE\\s+OF\\s+ART"
  )

  stringr::regex(paste(positive_patterns, collapse = "|"), ignore_case = TRUE)
}

is_pha_owner_name <- function(owner_name) {
  stringr::str_detect(normalize_owner_name(owner_name), pha_owner_regex())
}

is_university_owner_name <- function(owner_name) {
  stringr::str_detect(normalize_owner_name(owner_name), university_owner_regex())
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

# =============================================================================
# ALTOS BED-MIX STANDARDIZATION HELPERS
# =============================================================================

#' Collapse numeric bedrooms into Phase 1 Altos bed bins
#'
#' @param beds Numeric bedroom count
#' @return Character vector in {"studio","1br","2br","3plus"} or NA
altos_bed_bin <- function(beds) {
  beds_num <- suppressWarnings(as.numeric(beds))
  out <- rep(NA_character_, length(beds_num))
  out[is.finite(beds_num) & beds_num == 0] <- "studio"
  out[is.finite(beds_num) & beds_num == 1] <- "1br"
  out[is.finite(beds_num) & beds_num == 2] <- "2br"
  out[is.finite(beds_num) & beds_num >= 3] <- "3plus"
  out
}

#' Map detailed parcel building types to a fixed bed-mix class
#'
#' Phase 1 uses coarse classes for Altos composition standardization. Commercial,
#' non-residential, and missing types are conservatively treated as large-apartment
#' mix to avoid underweighting smaller-bedroom units.
#'
#' @param building_type Character vector from `standardize_building_type()`
#' @return Character vector in {"small_apartment","mid_apartment","large_apartment"}
altos_bed_mix_class <- function(building_type) {
  bt <- toupper(coalesce(as.character(building_type), ""))
  out <- rep("large_apartment", length(bt))

  out[bt %in% c("DETACHED", "ROW", "TWIN", "SMALL_MULTI_2_4")] <- "small_apartment"
  out[bt %in% c("LOWRISE_MULTI", "OTHER", "GROUP_QUARTERS")] <- "mid_apartment"
  out[bt %in% c("MIDRISE_MULTI", "HIGHRISE_MULTI", "MULTI_BLDG_COMPLEX", "COMMERCIAL")] <- "large_apartment"

  out
}

#' Default fixed bedroom shares by coarse structure class (Phase 1 fallback)
#'
#' These are used when an AHS-derived table is unavailable. Shares sum to 1 within
#' each `mix_class`.
#'
#' @return data.table with columns `mix_class`, `bed_bin`, `weight`, `weight_source`
default_altos_bed_mix_weights <- function() {
  dt <- data.table::data.table(
    mix_class = rep(c("small_apartment", "mid_apartment", "large_apartment"), each = 4),
    bed_bin = rep(c("studio", "1br", "2br", "3plus"), times = 3),
    weight = c(
      0.02, 0.22, 0.46, 0.30,  # small_apartment
      0.06, 0.38, 0.38, 0.18,  # mid_apartment
      0.14, 0.50, 0.28, 0.08   # large_apartment
    ),
    weight_source = "fallback_default_phase1"
  )
  dt[]
}

#' Parse AHS Table 0 bedroom-by-structure counts into Phase 1 bed-mix weights
#'
#' Expected source is the CSV export used in this repo (AHS Table 0). The parser
#' locates the `Bedrooms` section (header row ~189 in the current export), then
#' reads the rows `None`, `1`, `2`, `3`, and `4 or more`.
#'
#' Mapping from AHS structure columns to Phase 1 mix classes:
#' - `small_apartment` = `2 to 4 Units`
#' - `mid_apartment`   = `5 to 9 Units` + `10 to 19 Units`
#' - `large_apartment` = `20 to 49 Units` + `50 or more`
#'
#' `3plus` combines the AHS `3` and `4 or more` bedroom rows.
#'
#' @param ahs_path Path to AHS Table 0 CSV export
#' @return data.table with columns `mix_class`, `bed_bin`, `weight`, `weight_source`
parse_ahs_table0_bed_mix_weights <- function(ahs_path) {
  stopifnot(file.exists(ahs_path))

  ahs <- data.table::fread(
    ahs_path,
    header = FALSE,
    fill = TRUE,
    na.strings = c("", "NA"),
    encoding = "UTF-8"
  )
  setDT(ahs)

  # Clean UTF-8 BOM if present in the first cell.
  if ("V1" %in% names(ahs)) {
    ahs[, V1 := gsub("^\ufeff", "", as.character(V1))]
  }

  trim_chr <- function(x) trimws(as.character(x))
  parse_num <- function(x) {
    out <- suppressWarnings(as.numeric(gsub(",", "", trim_chr(x), fixed = TRUE)))
    out[!is.finite(out)] <- NA_real_
    out
  }

  # Column labels live in row 2 in this export.
  header_row <- 2L
  col_labels <- vapply(ahs[header_row], function(z) trim_chr(z), character(1))
  col_map <- stats::setNames(seq_along(col_labels), col_labels)
  col_var_map <- stats::setNames(names(ahs), col_labels)

  req_cols <- c("2 to 4 Units", "5 to 9 Units", "10 to 19 Units", "20 to 49 Units", "50 or more")
  missing_cols <- setdiff(req_cols, names(col_map))
  if (length(missing_cols) > 0) {
    stop("AHS parser: missing expected structure columns: ", paste(missing_cols, collapse = ", "))
  }

  bedrooms_hdr <- which(trim_chr(ahs$V1) == "Bedrooms")
  if (length(bedrooms_hdr) == 0) stop("AHS parser: could not find 'Bedrooms' section")
  bedrooms_hdr <- bedrooms_hdr[1]

  # Data rows begin after the blank row following the section header.
  start_row <- bedrooms_hdr + 2L
  row_labels <- trim_chr(ahs$V1)
  end_row <- which(seq_len(nrow(ahs)) > start_row & row_labels == "")[1] - 1L
  if (!is.finite(end_row)) end_row <- nrow(ahs)

  bed_block <- ahs[start_row:end_row]
  bed_block[, row_label := trim_chr(V1)]

  target_rows <- data.table::data.table(
    row_label = c("None", "1", "2", "3", "4 or more"),
    bed_bin_src = c("studio", "1br", "2br", "3", "4plus")
  )

  bed_rows <- merge(target_rows, bed_block, by = "row_label", all.x = TRUE, sort = FALSE)
  if (bed_rows[is.na(V1), .N] > 0) {
    stop("AHS parser: missing expected bedroom rows in 'Bedrooms' section")
  }

  counts_long <- rbindlist(list(
    data.table::data.table(
      bed_bin_src = bed_rows$bed_bin_src,
      mix_class = "small_apartment",
      count = parse_num(bed_rows[[col_var_map[["2 to 4 Units"]]]])
    ),
    data.table::data.table(
      bed_bin_src = bed_rows$bed_bin_src,
      mix_class = "mid_apartment",
      count = parse_num(bed_rows[[col_var_map[["5 to 9 Units"]]]]) +
        parse_num(bed_rows[[col_var_map[["10 to 19 Units"]]]])
    ),
    data.table::data.table(
      bed_bin_src = bed_rows$bed_bin_src,
      mix_class = "large_apartment",
      count = parse_num(bed_rows[[col_var_map[["20 to 49 Units"]]]]) +
        parse_num(bed_rows[[col_var_map[["50 or more"]]]])
    )
  ), use.names = TRUE)

  counts <- counts_long[
    ,
    .(count = sum(count, na.rm = TRUE)),
    by = .(mix_class, bed_bin = fifelse(bed_bin_src %in% c("3", "4plus"), "3plus", bed_bin_src))
  ]

  # Require all bins and positive totals.
  req_bins <- c("studio", "1br", "2br", "3plus")
  out <- CJ(
    mix_class = c("small_apartment", "mid_apartment", "large_apartment"),
    bed_bin = req_bins,
    unique = TRUE
  )
  out <- merge(out, counts, by = c("mix_class", "bed_bin"), all.x = TRUE)
  if (out[is.na(count), .N] > 0) {
    stop("AHS parser: missing counts for some (mix_class, bed_bin) cells")
  }

  out[, total_count := sum(count), by = mix_class]
  if (out[total_count <= 0 | !is.finite(total_count), .N] > 0) {
    stop("AHS parser: non-positive total count in at least one mix_class")
  }

  out[, weight := count / total_count]
  out[, weight_source := "ahs_table0_bedrooms_structure_2026"]
  out[, c("count", "total_count") := NULL]

  out[]
}

#' Load AHS-based bedroom shares for Altos standardization (optional)
#'
#' This function uses the configured AHS table when available; otherwise it falls
#' back to `default_altos_bed_mix_weights()` and logs the reason.
#'
#' @param cfg Optional config object from `read_config()`
#' @param input_key Config input key for the AHS table
#' @param log_file Optional log file path for `logf()`
#' @return data.table with columns `mix_class`, `bed_bin`, `weight`, `weight_source`
get_altos_bed_mix_weights <- function(cfg = NULL,
                                      input_key = "ahs_table0_bed_mix",
                                      log_file = NULL) {
  log_local <- function(...) {
    if (exists("logf", mode = "function")) {
      logf(..., log_file = log_file)
    }
  }

  weights <- default_altos_bed_mix_weights()

  # Optional config-driven AHS path. If absent/missing, fallback silently + log.
  ahs_path <- NULL
  if (!is.null(cfg) && !is.null(cfg$inputs) && !is.null(cfg$inputs[[input_key]])) {
    ahs_path <- if (exists("p_input", mode = "function")) {
      p_input(cfg, input_key)
    } else {
      cfg$inputs[[input_key]]
    }
  }

  if (is.null(ahs_path) || !nzchar(ahs_path)) {
    log_local("Altos bed-mix weights: using fallback defaults (no config input key '", input_key, "').")
  } else if (!file.exists(ahs_path)) {
    log_local("Altos bed-mix weights: using fallback defaults (AHS file missing: ", ahs_path, ").")
  } else {
    parsed <- tryCatch({
      parse_ahs_table0_bed_mix_weights(ahs_path)
    }, error = function(e) {
      log_local("Altos bed-mix weights: AHS parse failed, using fallback defaults. Error: ",
                conditionMessage(e))
      NULL
    })
    if (!is.null(parsed)) {
      weights <- parsed
      log_local("Altos bed-mix weights: loaded AHS-based weights from ", ahs_path)
    }
  }

  # Assertions: weights must be complete and sum to 1 within class
  req_bins <- c("studio", "1br", "2br", "3plus")
  req_cls <- c("small_apartment", "mid_apartment", "large_apartment")
  stopifnot(all(req_bins %in% weights$bed_bin))
  stopifnot(all(req_cls %in% weights$mix_class))
  chk <- weights[, .(weight_sum = sum(weight, na.rm = TRUE), n_bins = data.table::uniqueN(bed_bin)), by = mix_class]
  stopifnot(all(chk$mix_class %in% req_cls))
  stopifnot(all(abs(chk$weight_sum - 1) < 1e-8))
  stopifnot(all(chk$n_bins == length(req_bins)))

  weights[]
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
  "THE",
  # Additional near-certain non-person institution/commercial terms.
  "PHILADELPHIA", "SCHOOL", "SCHOOLS", "ACADEMY",
  "TAVERN", "INN", "HOTEL", "MOTEL", "BAR", "GRILL", "DINER", "CAFE",
  "UNIVERSITY", "COLLEGE", "HOSPITAL", "CLINIC",
  "CHURCH", "TEMPLE", "MOSQUE", "MINISTRY", "MINISTRIES",
  "DEPARTMENT", "DEPT", "AGENCY", "BUREAU", "OFFICE", "OFFICES",
  "CITY", "COUNTY", "BOROUGH", "TOWNSHIP", "COMMONWEALTH", "STATE",
  "BANK", "CREDIT", "UNION", "INSURANCE",
  # Additional business terms requested for non-person screening.
  "LAW", "AUTO", "COLLISION", "COLLISSION", "MOTORS", "GARAGE",
  "BODY", "REPAIR", "TOWING", "MECHANIC", "SERVICE", "SERVICES"
)

# Non-person/placeholder words. These are not all businesses, but are rarely
# person defendants and should trigger non-person handling in name pipelines.
non_person_words <- c(
  "UNKNOWN", "UNAUTHORIZED", "TENANT", "TENANTS", "RESIDENT", "RESIDENTS",
  "ESTATE", "ESTATES", "HEIR", "HEIRS", "ET AL", "AKA",
  "OCC", "OCCS", "OCCU", "OCCUP", "OCCUPS", "OCCSUPS",
  "OCCUPANT", "OCCUPANTS", "OCCUPANS", "OCCUPNTS", "OCCUPANTRS", "OCCUPANRTS", "OCCUPANRS",
  "ALL OTHER OCCUPANTS", "ALL OCCUPANTS", "ALL OTHERS", "ALL OTHER OCCS"
)

# Standardization function
# Create regex pattern (case insensitive)
business_regex <- regex(str_c("\\b(", str_c(business_words, collapse = "|"), ")\\b", collapse = "|"), ignore_case = T)
non_person_regex <- regex(str_c("\\b(", str_c(non_person_words, collapse = "|"), ")\\b", collapse = "|"), ignore_case = TRUE)
business_or_nonperson_regex <- regex(
  str_c("\\b(", str_c(unique(c(business_words, non_person_words)), collapse = "|"), ")\\b", collapse = "|"),
  ignore_case = TRUE
)

# Business-style initials pattern (e.g., "A & B"), with spouse abbreviations
# explicitly carved out via spousal_marker_regex.
initial_amp_initial_regex <- regex("\\b[A-Z]\\s*&\\s*[A-Z]\\b", ignore_case = TRUE)

# Spousal shorthand markers frequently included in defendant strings.
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

# Broad occupant-token matcher. This intentionally captures OCR/typing variants
# (e.g., OCCUAPNTS, OCCUPNATS) while excluding OCCUPATIONAL.
occupant_token_pattern <- "(?:OCC(?!UPATIONAL\\b)[A-Z]{0,12})"
occupant_token_regex <- regex(str_c("\\b", occupant_token_pattern, "\\b"), ignore_case = TRUE)

strip_occupant_phrases <- function(x) {
  x %>%
    # Handle split truncation patterns like "OCCU ANTS".
    str_replace_all(regex("\\bOCCU\\s+ANTS?\\b", ignore_case = TRUE), " ") %>%
    # Handle OCR truncation like "ALL OTHE ROCC" (for "ALL OTHER OCC...").
    str_replace_all(
      regex("\\b(?:AND\\s+)?ALL\\s+OTHER?\\s+ROCC\\b", ignore_case = TRUE),
      " "
    ) %>%
    # Remove broad occupant clauses and typo variants.
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
    # Remove "ET AL" style placeholders frequently attached to names.
    str_replace_all(regex("\\bET\\s+AL\\b", ignore_case = TRUE), " ") %>%
    str_squish()
}

strip_spousal_phrases <- function(x) {
  x %>%
    str_replace_all(spousal_marker_regex, " ") %>%
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
      str_detect(.,"(PHILA.+HOU\\s?S|HOUS.+ AUTH?|PHILA.+AUTH|ADELPHIA HOUSE)"),
      "PHILADELPHIA HOUSING AUTHORITY", .) %>%
    str_squish() %>%
    str_trim() %>%
    return()
}

clean_name <- function(name){
  name %>%
    str_replace_all("[’']", "") %>%
    strip_spousal_phrases() %>%
    # replace & with AND
    str_replace_all("([^\\s])&([^\\s])", "\\1 AND \\2") %>%
    str_replace_all("([^\\s]),([^\\s])", "\\1 , \\2") %>%
    str_replace_all("&", " AND ") %>%
    str_replace_all("\\|", " | ") %>%
    # strip punctuation
    str_replace_all("[[:punct:]]"," ") %>%
    str_squish() %>%
    strip_spousal_phrases() %>%
    strip_occupant_phrases() %>%
    fix_spellings() %>%
    strip_spousal_phrases() %>%
    strip_occupant_phrases() %>%
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
