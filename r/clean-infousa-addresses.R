philly_cols = fread("~/Desktop/data/philly-evict/parcel_address_cleaned.csv", nrows = 1) %>%
  select(n_sn_ss_c, contains('pm')) %>%
  colnames()

philly_infousa_dt = fread("~/Desktop/data/philly-evict/philly-infousa.csv")
philly_infousa_dt[,pm.zip := str_pad(zip, 5, "left", "0")]
philly_infousa_dt[,pm.city := str_to_lower(city)]
philly_infousa_dt[,pm.city := fifelse(pm.city == "phila", "philadelphia", pm.city)]
philly_infousa_dt[,pm.state := str_to_lower(state)]
philly_infousa_dt[,pm.street := str_to_lower(street_name)]
philly_infousa_dt[,pm.house := str_to_lower(house_num) %>% str_remove_all("[a-z]$") %>% str_squish()]
philly_infousa_dt[,pm.streetSuf := str_to_lower(street_suffix)]
# STREET_PRE_DIR
philly_infousa_dt[,pm.preDir := str_to_lower(street_pre_dir)]
# STREET_POST_DIR
philly_infousa_dt[,pm.sufDir := str_to_lower(street_post_dir)]
philly_infousa_dt[,pm.dir_concat := coalesce(pm.preDir, pm.sufDir)]
philly_infousa_dt[,n_sn_ss_c := str_squish( str_to_lower(paste(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir)))]
# replace nas
philly_infousa_dt[,n_sn_ss_c := fifelse(n_sn_ss_c == "na", NA_character_, n_sn_ss_c)]
philly_infousa_dt[,pm.uid := .GRP, by = .(n_sn_ss_c, pm.zip, pm.city, pm.state)]
fwrite(philly_infousa_dt[], "~/Desktop/data/philly-evict/infousa_address_cleaned.csv")

