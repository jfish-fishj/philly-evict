#### libraries
library(data.table)
library(tidyverse)
library(postmastr)
library(bit64)
library(sf)
library(geosphere)
library(DescTools)

altos = fread("~/Desktop/data/altos/cities/philadelphia_metro_all_years.csv")


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
# these are mostly idiosyncratic to boston altos data.
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


altos[,street_address_lower := str_to_lower(street_address) %>% misc_pre_processing()]


# setup for MA specific cleaning
dirs <- pm_dictionary(type = "directional", filter = c("N", "S", "E", "W"), locale = "us")
pa <- pm_dictionary(type = "state", filter = "PA", case = c("title", "upper","lower"), locale = "us")
# list of boston n'hoods. done to parse off city. e.g. 123 main st, dorchester ma
philly <- tibble(city.input = "philadelphia")



#### cleaning ####
# this is running on a sample for now until the code looks like it's working
# goal is that every altos listing should match a parcel

altos_sample = altos %>%
  #filter(street_address_lower %in% sample(street_address_lower, 25000)) %>%
  pm_identify(var = street_address_lower)

dropped_ids_og = altos_sample %>% filter(!pm.type %in% c("full", "short")) %>% pull(pm.uid)


altos_sample = altos_sample%>%
  filter(pm.type %in% c("full", "short"))

# workflow is
"
prep
parse unit
parse postal code
parse state
parse city
parse unit again
parse house #
parse street directionals
parse street suffiix
parse street name
post processing
  parse ranges of house numbers
  remove letters from house numbers
recombine
"
# altos gets rid of any address that's not an intersection, so save those for later
altos_adds_sample <- pm_prep(altos_sample, var = "street_address_lower", type = "street")

# dropped pm.uid
dropped_ids = altos_sample %>% filter(!pm.uid %in% altos_adds_sample$pm.uid) %>% pull(pm.uid)
# have to parse off unit first for 123 main st boston ma #1
# otherwise parsing gets all messed up
# general issue w/ postmastr is that mistakes cascade, so need to check
# parsing at each step
altos_adds_sample_units = parse_unit(altos_adds_sample$pm.address)
altos_adds_sample$pm.address = altos_adds_sample_units$clean_address %>% str_squish()
altos_adds_sample <- pm_postal_parse(altos_adds_sample, locale = "us")


# unit parsing -- 3 rounds
altos_adds_sample_units_r1 = parse_unit(altos_adds_sample$pm.address)
# parse units again
altos_adds_sample_units_r2 = parse_unit(altos_adds_sample_units_r1$clean_address%>% str_squish())
altos_adds_sample_units_r3 = parse_unit_extra(altos_adds_sample_units_r2$clean_address %>% str_squish())
altos_adds_sample$pm.address = altos_adds_sample_units_r2$clean_address %>% str_squish()
altos_adds_sample$pm.unit = altos_adds_sample_units_r3$unit
# save the unit tibble. rn i don't do anything with this since I don't use the
# unit number for merging but in theory usefule
unit_tibble = tibble(
  address = altos_adds_sample_units$og_address,
  u1 = altos_adds_sample_units$unit,
  u2 = altos_adds_sample_units_r1$unit,
  u3 = altos_adds_sample_units_r2$unit,
  u4 = altos_adds_sample_units_r3$unit,
  uid = altos_adds_sample$pm.uid
) %>%
  mutate(
    pm.unit = coalesce(u1, u2, u3, u4)
  )
altos_adds_sample = pm_house_parse(altos_adds_sample)
altos_adds_sample_nums = altos_adds_sample$pm.house %>% parse_letter()
altos_adds_sample_range = altos_adds_sample_nums$clean_address %>% parse_range()
new_pm_house = coalesce( altos_adds_sample_range$range1, altos_adds_sample_range$og_address )
altos_adds_sample$pm.house = new_pm_house %>% as.numeric()
altos_adds_sample$pm.house2 = altos_adds_sample_range$range2 %>% as.numeric()
altos_adds_sample$pm.house.letter = altos_adds_sample_nums$letter
altos_adds_sample_copy1 = copy(altos_adds_sample)

altos_adds_sample = pm_streetDir_parse(altos_adds_sample, dictionary = dirs)
altos_adds_sample = pm_streetSuf_parse(altos_adds_sample)
# have to copy pre parsing... bc pm_street_parse will drop a bunch of ids
altos_adds_sample_copy = copy(altos_adds_sample)
# okay so postmastr does this annoying thing where if the address is incomplete
# and is like 123 centre
# we know from boston context that centre is centre st
# so we should be able to move that centre from the streetsuffix column to the
# address column and fix it later
altos_adds_sample = altos_adds_sample %>%
  mutate(
    pm.city = "philadelphia",
    pm.state = "pa",
    pm.address = coalesce(pm.address, pm.streetSuf, pm.city, pm.state) %>%
      str_to_lower() ,
    pm.address  = case_when(
      pm.address  == "riv" ~ "river",
      pm.address  == "ctr" ~ "centre",
      pm.address  == "pk" ~ "park",
      pm.address  == "grv"~ "grove",
      pm.address == "gdn"~ "garden",
      pm.address == "smt"~ "summit",
      pm.address == "spg"~ "spring",
      TRUE ~ pm.address
    )
  )
altos_adds_sample = pm_street_parse(altos_adds_sample, ordinal= T, drop = F)
altos_adds_sample$pm.street = misc_post_processing(str_to_lower(altos_adds_sample$pm.street))

altos_adds_sample$pm.streetSuf = fifelse(
  str_detect(altos_adds_sample$pm.street, "\\s(st$)"),
  "st",
  altos_adds_sample$pm.streetSuf
) %>% str_squish()
altos_adds_sample$pm.street = fifelse(
  str_detect(altos_adds_sample$pm.street, "\\s(st$)"),
  str_replace_all(altos_adds_sample$pm.street, "\\sst$", ""),
  altos_adds_sample$pm.street
) %>% str_squish()
# more ids get dropped...
# View(altos_adds_sample_copy %>% filter(!pm.uid %in% altos_adds_sample$pm.uid ))
# View(altos_sample %>% filter(pm.uid %in% (altos_adds_sample_copy %>% filter(!pm.uid %in% altos_adds_sample$pm.uid ))$pm.uid)
#      %>% distinct(street_address, street_address_lower, pm.uid))

# should turn this into a function but whatever
altos_adds_sample$pm.streetSuf = altos_adds_sample$pm.streetSuf %>%
  str_replace_all("spark", " pk") %>%
  str_replace_all("street", " st") %>%
  str_replace_all("square", " sq") %>%
  str_replace_all("lane", " ln") %>%
  str_replace_all("alley", " aly") %>%
  str_replace_all("way", " way")

# make all components lower case and make composite address column
altos_adds_sample_c = altos_adds_sample %>%
  mutate(across(c( pm.sufDir, pm.street, pm.streetSuf, pm.preDir), ~str_squish(str_to_lower(replace_na(.x, ""))))) %>%
  mutate(
    n_sn_ss_c = str_squish( str_to_lower(paste(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir))),
    pm.dir_concat = coalesce(pm.preDir, pm.sufDir)
  )

setDT(altos_adds_sample_c)
setDT(altos_sample)


altos_adds_sample_c[,num_st_sfx_id := .GRP, by = .(pm.house, pm.street, pm.streetSuf)]




altos_adds_sample_c$pm.street = misc_post_processing(str_to_lower(altos_adds_sample_c$pm.street))

#### merges with address file ####
altos_adds_sample_c[,addy_id := .I]
altos_clean = altos_adds_sample_c %>%
  # recombine back w/ altos data
  merge(altos_sample[],
        by = "pm.uid",
        all.y = T)

# add the zipcode level data from altos onto the address data
# then create new id based on old ID + new zip code info
# eg 123 main st 12345 != 123 main st 99999
altos_clean[,pm.zip_imp := case_when(
  str_length(pm.zip) >= 4 ~ paste0("_",str_pad(pm.zip,5,"left","0")),
  str_length(zip) >= 4 ~ paste0("_",str_pad(zip,5,"left","0")),
  TRUE ~ NA_character_
)]
altos_clean[,num_zips := uniqueN(pm.zip_imp, na.rm = T), by = num_st_sfx_id]
altos_clean[,num_zips_uid := uniqueN(pm.zip_imp, na.rm = T), by = pm.uid]
altos_clean[,pm.uid_zip := .GRP, by = .(pm.uid, pm.zip_imp)]
altos_clean[,.N, by = num_zips_uid]
altos_clean[,num_listings := .N, by = .(beds, pm.uid)]
altos_clean[,num_years := uniqueN(year), by = .(beds, pm.uid)]

fwrite(altos_clean, "~/Desktop/data/altos/cities/philadelphia_metro_all_years_clean_addys.csv")
