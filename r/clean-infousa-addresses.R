philly_cols = fread("~/Desktop/data/philly-evict/parcel_address_cleaned.csv", nrows = 1) %>%
  select(n_sn_ss_c, contains('pm')) %>%
  colnames()
philly_cols
philly_infousa_dt[,pm.zip := str_pad(ZIP, 5, "left", "0")]
philly_infousa_dt[,pm.city := str_to_lower(CITY)]
philly_infousa_dt[,pm.state := str_to_lower(STATE)]
philly_infousa_dt[,pm.street := str_to_lower(STREET_NAME)]
philly_infousa_dt[,pm.house := str_to_lower(HOUSE_NUM)]
philly_infousa_dt[,pm.streetSuf := str_to_lower(STREET_SUFFIX)]
# STREET_PRE_DIR
philly_infousa_dt[,pm.preDir := str_to_lower(STREET_PRE_DIR)]
# STREET_POST_DIR
philly_infousa_dt[,pm.sufDir := str_to_lower(STREET_POST_DIR)]
philly_infousa_dt[,pm.dir_concat := coalesce(pm.preDir, pm.sufDir)]
philly_infousa_dt[,n_sn_ss_c := str_squish( str_to_lower(paste(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir)))]
# replace nas
philly_infousa_dt[,n_sn_ss_c := fifelse(n_sn_ss_c == "na", NA_character_, n_sn_ss_c)]
philly_infousa_dt[,pm.uid := .GRP, by = .(n_sn_ss_c, pm.zip, pm.city, pm.state)]
fwrite(philly_infousa_dt[], "~/Desktop/data/philly-evict/infousa_address_cleaned.csv")

