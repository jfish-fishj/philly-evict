library(data.table)
library(tidyverse)
library(sf)
library(tidycensus)
library(postmastr)

philly_hist = readRDS("/Users/joefish/Desktop/data/philly-evict/philadelphia_historical.rds")
philly_cur = readRDS("/Users/joefish/Desktop/data/philly-evict/philadelphia_2020_2021.rds")

setDT(philly_hist)
setDT(philly_cur)
cols = c("xcasenum", "xfileyear", "xfileweek","xfilemonth",
         "xdefendant","xplaintiff",
         "xdefendant_address", "xplaintiff_address",
         "GEOID.Tract","GEOID.latitude","GEOID.longitude","GEOID.Zip","GEOID.accuracy"  ,
         "sealed","dup","anon","include","commercial"
         )

philly_hist[,GEOID.Zip := zip]
philly_hist[,GEOID.longitude := Longitude]
philly_hist[,GEOID.latitude := Latitude]
philly_hist[,GEOID.accuracy := `Accuracy Score`]
philly_hist[, xdefendant_address := coalesce(xdefendant_street, xdefendant_address)]

philly_cur= philly_cur %>% unite(
 col = "xdefendant_address", xdefendant_number, xdefendant_street, sep = " ", na.rm = T, remove = F
)

philly_evict = bind_rows(philly_hist %>%
                           select(cols),
                         philly_cur %>%
                           select(cols)
                         ) %>%
  mutate(xdefendant_address = str_to_lower(xdefendant_address))

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


# setup for MA specific cleaning
dirs <- pm_dictionary(type = "directional", filter = c("N", "S", "E", "W"), locale = "us")
pa <- pm_dictionary(type = "state", filter = "PA", case = c("title", "upper","lower"), locale = "us")
# list of boston n'hoods. done to parse off city. e.g. 123 main st, dorchester ma
cities = c(philly_hist$City, philly_cur$xdefendant_city) %>% str_to_lower()
philly <-   tibble(city.input = unique(cities)) #%>%
philly_evict[,xdefendant_address := str_remove_all(xdefendant_address, ", philadelphia, pa") %>%
               str_replace_all("blv($|\\s)", "blvd")]
philly_evict_sample = philly_evict %>%
  #filter(xdefendant_address %in% sample(xdefendant_address, 25000)) %>%
  pm_identify(var = xdefendant_address)

dropped_ids_og = philly_evict_sample %>% filter(!pm.type %in% c("full", "short")) %>% pull(pm.uid)


# philly_evict_sample = philly_evict_sample%>%
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
# philly_evict gets rid of any address that's not an intersection, so save those for later
philly_evict_adds_sample <- pm_prep(philly_evict_sample, var = "xdefendant_address", type = "street")

philly_evict_adds_sample <- pm_postal_parse(philly_evict_adds_sample, locale = "us")
philly_evict_adds_sample <- pm_state_parse(philly_evict_adds_sample, dictionary = pa)
philly_evict_adds_sample <- pm_city_parse(philly_evict_adds_sample, dictionary = philly)

# dropped pm.uid
dropped_ids = philly_evict_sample %>% filter(!pm.uid %in% philly_evict_adds_sample$pm.uid) %>% pull(pm.uid)
# have to parse off unit first for 123 main st boston ma #1
# otherwise parsing gets all messed up
# general issue w/ postmastr is that mistakes cascade, so need to check
# parsing at each step
philly_evict_adds_sample_units = parse_unit(philly_evict_adds_sample$pm.address)
philly_evict_adds_sample$pm.address = philly_evict_adds_sample_units$clean_address %>% str_squish()


# unit parsing -- 3 rounds
philly_evict_adds_sample_units_r1 = parse_unit(philly_evict_adds_sample$pm.address)
# parse units again
philly_evict_adds_sample_units_r2 = parse_unit(philly_evict_adds_sample_units_r1$clean_address%>% str_squish())
philly_evict_adds_sample_units_r3 = parse_unit_extra(philly_evict_adds_sample_units_r2$clean_address %>% str_squish())
philly_evict_adds_sample$pm.address = philly_evict_adds_sample_units_r3$clean_address %>% str_squish()
philly_evict_adds_sample$pm.unit = philly_evict_adds_sample_units_r3$unit
# save the unit tibble. rn i don't do anything with this since I don't use the
# unit number for merging but in theory usefule
unit_tibble = tibble(
  address = philly_evict_adds_sample_units$og_address,
  clean_address = philly_evict_adds_sample_units_r3$clean_address,
  u1 = philly_evict_adds_sample_units$unit,
  u2 = philly_evict_adds_sample_units_r1$unit,
  u3 = philly_evict_adds_sample_units_r2$unit,
  u4 = philly_evict_adds_sample_units_r3$unit,
  uid = philly_evict_adds_sample$pm.uid
) %>%
  mutate(
    pm.unit = coalesce(u1, u2, u3, u4)
  )


philly_evict_adds_sample = pm_house_parse(philly_evict_adds_sample)
philly_evict_adds_sample_nums = philly_evict_adds_sample$pm.house %>% parse_letter()
philly_evict_adds_sample_range = philly_evict_adds_sample_nums$clean_address %>% parse_range()
new_pm_house = coalesce( philly_evict_adds_sample_range$range1, philly_evict_adds_sample_range$og_address )
philly_evict_adds_sample$pm.house = new_pm_house %>% as.numeric()
philly_evict_adds_sample$pm.house2 = philly_evict_adds_sample_range$range2 %>% as.numeric()
philly_evict_adds_sample$pm.house.letter = philly_evict_adds_sample_nums$letter
philly_evict_adds_sample_copy1 = copy(philly_evict_adds_sample)

philly_evict_adds_sample = pm_streetDir_parse(philly_evict_adds_sample, dictionary = dirs)
philly_evict_adds_sample = pm_streetSuf_parse(philly_evict_adds_sample)
# have to copy pre parsing... bc pm_street_parse will drop a bunch of ids
philly_evict_adds_sample_copy = copy(philly_evict_adds_sample)

# have to peel off trailing letters
philly_evict_adds_sample = philly_evict_adds_sample %>% mutate(
  pm.address = pm.address %>% str_remove_all( "(\\s[a-z]{1}|1st|2nd)$")
)

philly_evict_adds_sample = pm_street_parse(philly_evict_adds_sample, ordinal= T, drop = F)

# fix some of the street names
philly_evict_adds_sample = philly_evict_adds_sample %>% mutate(
  pm.street = case_when(
    str_detect(pm.street, "[a-zA-Z]") ~ pm.street,
    pm.street == 1~ "1st",
    pm.street == 2~ "2nd",
    pm.street == 3~ "3rd",
    (pm.street )>=4 ~ paste0((pm.street ),"th"),
    TRUE ~ pm.street

  )
)

philly_evict_adds_sample = philly_evict_adds_sample %>% mutate(
  pm.street = case_when(
    pm.street == "berkeley" ~ "berkley",
    TRUE ~ pm.street
  )
)

philly_evict_adds_sample = philly_evict_adds_sample %>% mutate(
  pm.street = pm.street %>%
    str_replace_all("[Mm]t ","mount ") %>%
    str_replace_all("[Mm]c ","mc") %>%
    str_replace_all("[Ss]t ","saint ")

)


# make all components lower case and make composite address column
philly_evict_adds_sample_c = philly_evict_adds_sample %>%
  mutate(across(c( pm.sufDir, pm.street, pm.streetSuf, pm.preDir), ~str_squish(str_to_lower(replace_na(.x, ""))))) %>%
  mutate(
    n_sn_ss_c = str_squish( str_to_lower(paste(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir))),
    pm.dir_concat = coalesce(pm.preDir, pm.sufDir)
  )

setDT(philly_evict_adds_sample_c)
setDT(philly_evict_sample)

# merge and export
philly_evict_adds_sample_m = merge(
  philly_evict_sample,
  philly_evict_adds_sample_c,
  by.x = "pm.uid",
  by.y = "pm.uid",
  all.x = T
)

fwrite(philly_evict_adds_sample_m, "/Users/joefish/Desktop/data/philly-evict/evict_address_cleaned.csv")


