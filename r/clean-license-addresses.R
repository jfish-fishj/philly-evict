library(data.table)
library(tidyverse)
library(sf)
library(tidycensus)
library(postmastr)

philly_lic = fread("/Users/joefish/Desktop/data/philly-evict/business_licenses.csv")

# clean addresses
# parse unit from address column. returns unit column, original address, and
# cleaned address column (removes unit)
parse_unit <- function(address) {
  .regex = regex(
    "\\s(((fl|floor|-?apt\\.?|-?apartment\\.?|unit|suite|ste|rm|room|#|rear|building|bldg)+\\s?#?\\s?[0-9A-Z-]+)|\\s[0-9]{1,4})$",
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
    "([0-9]+-\\s([a-z\\s]+)\\s(homes?|apartments?))|(-[a-z][0-9]$)|([0-9]+[a-z]{1})$",
    ignore_case = T
  )
  matches = str_match(address, .regex)
  clean_address = str_remove(address, .regex) %>%
    str_replace_all("\\s(fl|floor|apt|apartment|unit|suite|ste|rm|room|#|rear|-|basement|bsmt)+$","")

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


# setup for PA specific cleaning
dirs <- pm_dictionary(type = "directional", filter = c("N", "S", "E", "W"), locale = "us")
pa <- pm_dictionary(type = "state", filter = "PA", case = c("title", "upper","lower"), locale = "us")


# list of boston n'hoods. done to parse off city. e.g. 123 main st, dorchester ma
#cities = c(philly_hist$City, philly_cur$xdefendant_city) %>% str_to_lower()
philly <-   tibble(city.input = "philadelphia") #%>%
philly_lic[,address := str_remove_all(address, ", philadelphia, pa")]
philly_lic_sample = philly_lic %>%
  #filter(address %in% sample(address, 25000)) %>%
  pm_identify(var = address)

dropped_ids_og = philly_lic_sample %>% filter(!pm.type %in% c("full", "short")) %>% pull(pm.uid)

#
# philly_lic_sample = philly_lic_sample%>%
#   filter(pm.type %in% c("full", "short"))

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
# philly_lic gets rid of any address that's not an intersection, so save those for later
philly_lic_adds_sample <- pm_prep(philly_lic_sample, var = "address", type = "street")

philly_lic_adds_sample <- pm_postal_parse(philly_lic_adds_sample, locale = "us")
philly_lic_adds_sample <- pm_state_parse(philly_lic_adds_sample, dictionary = pa)
philly_lic_adds_sample <- pm_city_parse(philly_lic_adds_sample, dictionary = philly)

# dropped pm.uid
dropped_ids = philly_lic_sample %>% filter(!pm.uid %in% philly_lic_adds_sample$pm.uid) %>% pull(pm.uid)
# have to parse off unit first for 123 main st boston ma #1
# otherwise parsing gets all messed up
# general issue w/ postmastr is that mistakes cascade, so need to check
# parsing at each step
philly_lic_adds_sample_units = parse_unit(philly_lic_adds_sample$pm.address)
philly_lic_adds_sample$pm.address = philly_lic_adds_sample_units$clean_address %>% str_squish()


# unit parsing -- 3 rounds
philly_lic_adds_sample_units_r1 = parse_unit(philly_lic_adds_sample$pm.address)
# parse units again
philly_lic_adds_sample_units_r2 = parse_unit(philly_lic_adds_sample_units_r1$clean_address%>% str_squish())
philly_lic_adds_sample_units_r3 = parse_unit_extra(philly_lic_adds_sample_units_r2$clean_address %>% str_squish())
philly_lic_adds_sample$pm.address = philly_lic_adds_sample_units_r3$clean_address %>% str_squish()
philly_lic_adds_sample$pm.unit = philly_lic_adds_sample_units_r3$unit
# save the unit tibble. rn i don't do anything with this since I don't use the
# unit number for merging but in theory usefule
unit_tibble = tibble(
  address = philly_lic_adds_sample_units$og_address,
  clean_address = philly_lic_adds_sample_units_r3$clean_address,
  u1 = philly_lic_adds_sample_units$unit,
  u2 = philly_lic_adds_sample_units_r1$unit,
  u3 = philly_lic_adds_sample_units_r2$unit,
  u4 = philly_lic_adds_sample_units_r3$unit,
  uid = philly_lic_adds_sample$pm.uid
) %>%
  mutate(
    pm.unit = coalesce(u1, u2, u3, u4)
  )


philly_lic_adds_sample = pm_house_parse(philly_lic_adds_sample)
philly_lic_adds_sample_nums = philly_lic_adds_sample$pm.house %>% parse_letter()
philly_lic_adds_sample_range = philly_lic_adds_sample_nums$clean_address %>% parse_range()
new_pm_house = coalesce( philly_lic_adds_sample_range$range1, philly_lic_adds_sample_range$og_address )
philly_lic_adds_sample$pm.house = new_pm_house %>% as.numeric()
philly_lic_adds_sample$pm.house2 = philly_lic_adds_sample_range$range2 %>% as.numeric()
philly_lic_adds_sample$pm.house.letter = philly_lic_adds_sample_nums$letter
philly_lic_adds_sample_copy1 = copy(philly_lic_adds_sample)

philly_lic_adds_sample = pm_streetDir_parse(philly_lic_adds_sample, dictionary = dirs)
philly_lic_adds_sample = pm_streetSuf_parse(philly_lic_adds_sample)
# have to copy pre parsing... bc pm_street_parse will drop a bunch of ids
philly_lic_adds_sample_copy = copy(philly_lic_adds_sample)

# have to peel off trailing letters
philly_lic_adds_sample = philly_lic_adds_sample %>% mutate(
  pm.address = str_remove_all(pm.address, "(\\s[a-z]{1}|1st|2nd)$")
)

philly_lic_adds_sample = pm_street_parse(philly_lic_adds_sample, ordinal= T, drop = F)


# make all components lower case and make composite address column
philly_lic_adds_sample_c = philly_lic_adds_sample %>%
  mutate(across(c( pm.sufDir, pm.street, pm.streetSuf, pm.preDir), ~str_squish(str_to_lower(replace_na(.x, ""))))) %>%
  mutate(
    n_sn_ss_c = str_squish( str_to_lower(paste(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir))),
    pm.dir_concat = coalesce(pm.preDir, pm.sufDir)
  )

setDT(philly_lic_adds_sample_c)
setDT(philly_lic_sample)

# merge and export
philly_lic_sample_m = merge(
  philly_lic_sample,
  philly_lic_adds_sample_c,
  by = "pm.uid", all.x = T
)

fwrite(philly_lic_sample_m, "/Users/joefish/Desktop/data/philly-evict/business_licenses_clean.csv")


