#### libraries ####
library(data.table)
library(tidyverse)
library(postmastr)
library(bit64)
library(glue)
library(janitor)
print("loaded libraries")

#### directories ####
print("setting directories")
input_dir = "~/Desktop/data/corelogic/county-data"
output_dir = '/Users/joefish/Desktop/data/corelogic/cleaned-addresses'
fips_code_list = list.files(input_dir) %>% str_extract("(?<=filtered_)[0-9]{5}(?=\\.csv)")

# loop through fips codes
print("looping through fips codes")
for(fips_code in fips_code_list){
  print(glue("processing {fips_code}"))
#### files ####

input_file = glue("{input_dir}/filtered_{fips_code}.csv")
output_file = glue("{output_dir}/cleaned_{fips_code}.csv")
# skip if output file exists
if(file.exists(output_file)){
  print(glue("Output file {output_file} exists, skipping"))
  next
}
corelogic_cols = fread(input_file, nrows = 1) %>%
  select(CLIP, `FIPS CODE`, `APN (PARCEL NUMBER UNFORMATTED)`, `ORIGINAL APN`, RANGE, `CENSUS ID`, TOWNSHIP, `MUNICIPALITY NAME`,
         `MUNICIPALITY CODE`, `SITUS CORE BASED STATISTICAL AREA (CBSA)`:`SITUS STREET ADDRESS`,
          `TOTAL VALUE CALCULATED`:`TAX EXEMPT AMOUNT TOTAL`,
         `FRONT FOOTAGE`:last_col() ) %>%
  # select(clip, fips_code, apn_parcel_number_unformatted, original_apn, range, census_id, township, municipality_name, municipality_code,
  #        situs_core_based_statistical_area_cbsa:situs_street_address, total_value_calculated:tax_exempt_amount_total,
  #        front_footage:last_col()
  #        ) %>%
  colnames()

corelogic = fread(input_file, select = corelogic_cols)
corelogic = corelogic %>% janitor::clean_names()

#### functions ####
# expand address range. e.g. 123-133 main st becomes 6 rows for 123,125,127, 129,131, 133
# even odd means expand only on even/odd numbers
expand_addresses <- function(.data, num1, num2, max_addr = 2000, even_odd =T){
  .data[,num_addresses := fifelse(!is.na(get(num2)),get(num2) - get(num1), 1)]
  if( even_odd ==T){
    .data[,num_addresses := (num_addresses %/% 2)+1]
  }
  .data[,num_addresses := fifelse(num_addresses <= 0 |is.na(num_addresses) , 1, num_addresses)]
  .data[,num_addresses := fifelse(num_addresses > max_addr , max_addr, num_addresses)]
  .data[,row_id := seq(.N)]
  .data = .data %>% uncount(weights = num_addresses) %>% as.data.table()
  .data[,address_num := seq(.N), by = row_id]
  .data[,num1 := get(num1) + 2*(address_num - 1)]
  # if(even_odd == T){
  #   # filter for just addresses that are on one side of street. e.g.
  #   # 123-133 main st becomes 6 rows for 123,125,127, 129,131, 133 and not every
  #   # number in that range
  #   .data = .data[address_num %% 2 ==1]
  # }
  return(.data)
}


# parse unit from address column. returns unit column, original address, and
# cleaned address column (removes unit)
parse_unit <- function(address) {
  .regex = regex(
    "(((fl|floor|apt|apartment|unit|suite|ste|rm|room|#|rear)+\\s?#?\\s?[0-9A-Z-]+)|\\s[0-9]+)$",
    ignore_case = T
  )
  matches = str_match(address, .regex)
  clean_address = str_remove(address, .regex)
  return(tibble(
    clean_address = clean_address,
    unit = matches[, 2],
    og_address = address
  ))

}

# same thing as parse unit but uses a different regex. this is designed to run
# after the first parse_unit function
parse_unit_extra <- function(address) {
  .regex = regex(
    "(([0-9]+)?([a-z]+-?[0-9]+))|(([a-z]+)?([0-9]+)-?[a-z]+)|(([0-9]+)-([0-9]+))$",
    ignore_case = T
  )
  matches = str_match(address, .regex)
  clean_address = str_remove(address, .regex) %>%
    str_replace_all("\\s(fl|floor|apt|apartment|unit|suite|ste|rm|room|#|rear|-|basement|bsmt)+$","")
  # have to fix the issue where 123 6th -> 123 bc people forgot to write
  # 6th street
  clean_address = fifelse(str_detect(address, "(st|nd|th|rd)[\\s$]"),address, clean_address)
  return(tibble(
    clean_address = clean_address,
    unit = matches[, 2],
    og_address = address
  ))
}


# parse range of addresses. turn 123-125 into a 123 and 125. returns 3 columns
# num1 and num2 and og address
parse_range <- function(address_num){
  .regex = regex(
    "([0-9]+)(\\s?-\\s?)([0-9]+)"
  )
  matches = str_match(address_num, .regex)
  return(tibble(
    range1 = matches[,2],
    range2 = matches[, 4],
    og_address = address_num
  ))
}

# changes 123A into 123 and A
parse_letter <- function(address_num){
  .regex = regex(
    "([0-9\\s-]+)([a-z]+)?$", ignore_case = T
  )
  matches = str_match(address_num, .regex)
  return(tibble(
    clean_address = matches[,2],
    letter = matches[, 3],
    og_address = address_num
  ))
}

# random things I do to clean the address column before parsing
# these are mostly idiosyncratic to boston corelogic data.
misc_pre_processing <- function(address_col){
  address_col %>%
    str_remove_all("1/2") %>%
    str_remove_all("&\\s[0-9]+") %>%
    str_remove_all("\\.") %>%
    str_remove_all("penthouse") %>%
    str_remove_all("full parking") %>%
    str_remove_all("\\(") %>%
    str_remove_all("\\)") %>%
    str_remove_all("x+[0-9]") %>%
    str_remove_all("\\s?n/a") %>%
    str_remove_all(" at ") %>%
    #str_remove_all("no fee|no tenant|parking|room for rent|new renovation|renovation|furn|furnished|near t|t stop|copy") %>%
    #str_replace_all("centre", "center") %>%
    str_replace_all("#+", "#") %>%
    str_replace_all("^([0-9]+)\\s([0-9]+)\\s", "\\1-\\2\\s") %>%
    str_squish()%>%
    str_replace_all("([0-9]+)/([0-9]+)", "\\1\\2") %>%
    str_replace_all("\\s?-\\s?", "-") %>%
    # big section of standardizations
    # what's happening is WOA address parsing only parses
    # hyde park square into SN:hyde and SS:park wheres this parser
    # does SN:Hyde Park and SS: Square
    # str_replace_all("hyde park avenue", "hyde park") %>%
    # str_replace_all("hyde park ave", "hyde park") %>%
    # str_replace_all("union park (st|street)", "union park") %>%
    # str_replace_all("sqnue", "square") %>%
    # str_replace_all("pkreet", "street") %>%
    # str_replace_all("pknue", "avenue") %>%
    # str_replace_all("oak square (ave|avenue)", "oak square") %>%
    # str_replace_all("spring park (ave|avenue)", "spring park") %>%
    # str_replace_all("highland park (ave|avenue)", "highland park") %>%
    # str_replace_all("marina (park|pk) (drive|dr)", "marina park") %>%
    # str_replace_all("hood park (drive|dr)", "hood park") %>%
    # str_replace_all("cedar lane way", "cedar lane") %>%
    # str_replace_all("hull (street|st) (court|ct)", "hull street") %>%
    # str_replace_all("pier (four|4) (blvd|boulevard)", "pier four") %>%
    str_replace_all("saint", "st") %>%
    str_replace_all("apt#$", "") %>%
    str_replace_all("ng$", "") %>%
    str_replace_all("bs$", "") %>%
    str_replace_all("\\sph(\\s|$)", "\\s") %>%
    str_replace_all("\\s[abcdfghijk]$", "") %>% # pretty common unit names. gets weird bc sometimes street names are letters
    str_replace("place", "pl") %>%
    str_squish() %>%
    str_replace_all("\\s(fl|floor|apt|apartment|unit|suite|ste|rm|room|#|rear|-)+$","") %>%
    return()
}

# matching address standardizations between postmastr and WOA
misc_post_processing <- function(address){
  address %>%
    str_replace_all("[\\s^]park", " pk") %>%
    str_replace_all("\\sstreet", " st") %>%
    str_replace_all("\\ssquare", " sq") %>%
    str_replace_all("\\slane", " ln") %>%
    str_replace_all("\\salley", " aly") %>%
    str_replace_all("\\sway", " way") %>%
    #str_replace_all("center", " centre") %>%
    str_replace_all("fm\\s", "farm to market ") %>%
    str_replace_all("pier four", "pier 4") %>%
    str_replace_all("centre", "center") %>%
    str_replace_all("mt", "mount") %>%
    #str_replace_all("\\shill", " hl") %>%
    return()
}

# standardize_st_name
# function to standardize street names between WOA and postmastr
standardize_st_name <- function(street_name){
  addys = tibble(
    sn = street_name,
    pm.id = seq(1,length(street_name)),
    pm.uid = seq(1,length(street_name))
  )
  st_tibble =  addys %>%
    pm_identify(var = sn) %>%
    pm_prep(var = "sn", type = "street") %>%
    pm_street_std(var = "pm.address") %>%
    mutate(pm.street = pm.address) %>%
    pm_replace(source = addys) %>%
    mutate(
      pm.street = coalesce(pm.street, sn) %>% str_to_lower(),
      pm.street = fifelse(pm.street == "center", "centre", pm.street),
      pm.street = str_replace_all(pm.street, "saint", "st"),
      pm.street = str_replace_all(pm.street, "farm to market road", "farm to market")
    )
  return(st_tibble$pm.street)
}

# city specific stgandardizations
standardize_st_name_houston <- function(street_name){
  street_name %>%
    str_replace_all("farm to market road", "farm to market") %>%
    str_replace_all( "farm to market road", "farm to market") %>%
    str_replace_all( "farm to market 196$", "farm to market 1960") %>%
    str_replace_all( "farm to market 292$", "farm to market 2920") %>%
    str_replace_all("^street", "st") %>%
    return()

}

standardize_st_name_boston <- function(street_name){
  street_name %>%
    str_replace_all("centre", "center") %>%
    str_replace_all( "^pier$", "pier 4") %>%
    str_replace_all( "mount", "mt") %>%
    str_replace_all( "saint", "st") %>%
    return()

}



#### pre processing ####
# skip most of the usual parsing bc corelogic comes pre parsed

corelogic[,street_address_lower := str_to_lower(situs_street_address) %>% misc_pre_processing()]
corelogic[,pm.zip_imp  :=paste0("_",str_pad(situs_zip_code,5,"left","0"))]
corelogic[,pm.city_imp := str_to_lower(situs_city) %>% str_squish()]
corelogic[,pm.state_imp := str_to_lower(situs_state) %>% str_squish()]
corelogic[,pm.uid := .GRP, by = street_address_lower]
corelogic[,pm.address := street_address_lower]
corelogic[,pm.house := situs_house_number]
corelogic[,pm.street := situs_street_name %>% str_to_lower() %>% misc_post_processing()]
corelogic[,pm.streetSuf := standardize_st_name(situs_mode)]
corelogic[,pm.preDir := situs_direction] # assumes no suf directional
corelogic[,pm.sufDir := NA_character_]
corelogic[,pm.dir_concat := situs_direction]
corelogic[,n_sn_ss_c := str_squish( str_to_lower(paste(pm.house, pm.preDir, pm.street, pm.streetSuf)))]

fwrite(corelogic,output_file )
}
