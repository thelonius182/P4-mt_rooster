# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# Add 1 new week to "Montagerooster" ---- 
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
library(dplyr)
library(tidyr)
library(lubridate)
library(magrittr)
library(chron)
library(stringr)
library(yaml)
library(purrr)
library(futile.logger)

filter <- dplyr::filter # avoid collision with stats::filter

flog.appender(appender.file("/Users/nipper/Logs/mt_rooster.log"), name = "mtr_log")
flog.info("= = = = = Montagerooster = = = = =", name = "mtr_log")

config <- read_yaml("config.yaml")

# read tbl_montage ----
source("src/get_google_czdata.R", encoding = "UTF-8")

# Find start date new week ----
# i.e. of Thursday 13:00 to start new week of MT-rooster (mtr)
# Reasoning: cz-week runs Th13-Th13, so current aas ends either on We or Th.
#            if We, then start = max(mtr) + 1, else start = max(mtr)
mtr_max <- max(tbl_montage$Uitzending)
mtr_start_date <-
  if_else(wday(mtr_max, label = T, abbr = T) == "do", # legend: 1 = zo
          mtr_max,
          mtr_max + days(1L))

# retrieve all info like it is done for "P1-wpgidsweek". Sharing this at code-level
# will be a future excercise :)
current_run_start <- ymd(mtr_start_date)
flog.info("Dit wordt de MT-roosterweek van %s", 
          format(current_run_start, "%A %d %B %Y"),
          name = "mtr_log")

# cz-week's 168 hours comprise 8 weekdays, not 7 (Thursday AM and PM)
# but to the schedule-template both Thursdays are the same, as the
# template is undated.
# Both Thursday parts will separate when the schedule gets 'calendarized'
current_run_stop <- current_run_start + ddays(7)

# create time series ------------------------------------------------------
cz_slot_days <- seq(from = current_run_start, to = current_run_stop, by = "days")
cz_slot_hours <- seq(0, 23, by = 1)
cz_slot_dates <- merge(cz_slot_days, chron(time = paste(cz_slot_hours, ":", 0, ":", 0)))
colnames(cz_slot_dates) <- c("slot_date", "slot_time")
cz_slot_dates$date_time <- as.POSIXct(paste(cz_slot_dates$slot_date, cz_slot_dates$slot_time))
row.names(cz_slot_dates) <- NULL

# + create cz-slot prefixes and combine with calendar ---------------------
cz_slot_dates_raw <- as.data.frame(cz_slot_dates) %>%
  select(date_time) %>%
  mutate(ordinal_day = 1 + (day(date_time) - 1) %/% 7L,
         cz_slot_pfx = paste0(wday(date_time, label = T, abbr = T),
                              ordinal_day,
                              "_",
                              str_pad(hour(date_time),
                                      width = 2,
                                      side = "left",
                                      pad = "0"
                              ))
  ) %>%
  select(-ordinal_day) %>% 
  arrange(date_time)

# read schedule template --------------------------------------------------
tbl_zenderschema <- readRDS(paste0(config$giva.rds.dir, "zenderschema.RDS"))

# create factor levels ----------------------------------------------------
cz_slot_key_levels = c(
  "titel",
  "product",
  "product A",
  "product B",
  "in mt-rooster",
  "cmt mt-rooster",
  "herhaling",
  "balk",
  "size"
)

weekday_levels = c("do", "vr", "za", "zo", "ma", "di", "wo")

# = = = = = = = = = = = = = = = = = = = = = = = = + = = = = = = = = = -----

# prep the weekly's, under 'week-0' ---------------------------------------
week_0_init <- tbl_zenderschema %>%
  select(cz_slot = slot,
         hh_formule,
         wekelijks,
         product_wekelijks,
         bijzonderheden_wekelijks,
         AB_cyclus,
         product_A_cyclus,
         product_B_cyclus,
         bijzonderheden_A_cyclus,
         bijzonderheden_B_cyclus,
         balk,
         balk_tonen
  )

# + pivot-long to collapse week attributes into NV-pairs ------------------
week_0_long1 <- gather(week_0_init, slot_key, slot_value, -cz_slot, na.rm = T) %>% 
  mutate(slot_key = case_when(slot_key == "wekelijks" ~ "titel", 
                              slot_key == "hh_formule" ~ "herhaling",
                              slot_key == "bijzonderheden_wekelijks" ~ "cmt mt-rooster",
                              slot_key == "bijzonderheden_A_cyclus" ~ "cmt mt-rooster",
                              slot_key == "bijzonderheden_B_cyclus" ~ "cmt mt-rooster",
                              slot_key == "product_wekelijks" ~ "product",
                              slot_key == "balk_tonen" ~ "in mt-rooster",
                              slot_key == "AB_cyclus" ~ "titel",
                              slot_key == "product_A_cyclus" ~ "product A",
                              slot_key == "product_B_cyclus" ~ "product B",
                              T ~ slot_key),
         slot_key = factor(slot_key, ordered = T, levels = cz_slot_key_levels),
         cz_slot_day = str_sub(cz_slot, 1, 2),
         cz_slot_day = factor(cz_slot_day, ordered = T, levels = weekday_levels),
         cz_slot_hour = str_sub(cz_slot, 3, 4),
         cz_slot_size = str_sub(cz_slot, 5),
         ord_day_1 = 1,
         ord_day_2 = 2,
         ord_day_3 = 3,
         ord_day_4 = 4,
         ord_day_5 = 5
  ) %>% 
  arrange(cz_slot_day, cz_slot_hour, slot_key)

# + do 5 copies of each slot, one for each ordinal day --------------------
week_0_long2 <-
  gather(
    data = week_0_long1,
    key = ord_day_tag,
    value = ord_day, 
    -starts_with("cz_"), -starts_with("slot_")
  ) %>%
  select(
    cz_slot_day,
    ord_day,
    cz_slot_hour,
    cz_slot_key = slot_key,
    cz_slot_value = slot_value,
    cz_slot_size
  ) %>% 
  arrange(cz_slot_day, ord_day, cz_slot_hour, cz_slot_key)

# + promote slot-size to be an attribute too ------------------------------
week_temp <- week_0_long2 %>% 
  select(-cz_slot_key, -cz_slot_value) %>% 
  mutate(cz_slot_key = "size",
         cz_slot_key = factor(x = cz_slot_key, ordered = T, levels = cz_slot_key_levels),
         cz_slot_value = cz_slot_size) %>% 
  select(cz_slot_day, ord_day, cz_slot_hour, cz_slot_key, cz_slot_value, cz_slot_size) %>% 
  distinct

# + final result week-0 ---------------------------------------------------
week_0 <- bind_rows(week_0_long2, week_temp) %>% 
  select(-cz_slot_size) %>% 
  arrange(cz_slot_day, ord_day, cz_slot_hour, cz_slot_key)

rm(week_0_init, week_0_long1, week_0_long2, week_temp)

# = = = = = = = = = = = = = = = = = = = = = = = = + = = = = = = = = = -----

# prep week-1/5 (days 1-7/8-14/... of month) ------------------------------
# no matter which weekday comes first!

source("src/wp_gidsweek_functions.R")

week_1 <-  prep_week("1")
week_2 <-  prep_week("2")
week_3 <-  prep_week("3")
week_4 <-  prep_week("4")
week_5 <-  prep_week("5")

# + merge the weeks, repair Thema -----------------------------------------

cz_week <- bind_rows(week_0, week_1, week_2, week_3, week_4, week_5) %>% 
  mutate(cz_slot_pfx = paste0(cz_slot_day, ord_day, "_", cz_slot_hour)) %>% 
  select(cz_slot_pfx, cz_slot_key, cz_slot_value) %>% 
  arrange(cz_slot_pfx, cz_slot_key) %>% 
  distinct %>% 
  # repair Thema
  filter(!cz_slot_pfx %in%  c("wo2_21", "wo4_21")) %>% 
  mutate(cz_slot_value = if_else(cz_slot_pfx %in% c("wo2_20", "wo4_20") 
                                 & cz_slot_key == "size",
                                 "120",
                                 cz_slot_value)
  )

rm(week_0, week_1, week_2, week_3, week_4, week_5, tbl_zenderschema)

# shrink date list Thursdays ----------------------------------------------
# list already runs  Thursday to Thursday; make it run from 13:00 - 13:00
df_start <- min(cz_slot_dates$date_time)
hour(df_start) <- 13
df_stop <- max(cz_slot_dates$date_time)
hour(df_stop) <- 13

cz_slot_dates <- cz_slot_dates_raw %>% 
  filter(date_time >= df_start & date_time < df_stop)

rm(cz_slot_dates_raw)

# = = = = = = = = = = = = = = = = = = = = = = = = + = = = = = = = = = -----

# join dates, slots & info ------------------------------------------------
# + get cycle for A/B-week ------------------------------------------------
cycle <- get_cycle(current_run_start)

# + get gidsinfo ----------------------------------------------------------
# tbl_gidsinfo <- readRDS(paste0(config$giva.rds.dir, "gidsinfo.RDS"))
# tbl_gidsinfo_nl_en <- readRDS(paste0(config$giva.rds.dir, "gidsinfo_nl_en.RDS"))

# + exclude this week's repeating broadcasts ------------------------------
#   This week's iTunes-titles where cycle says "repeat", should be excluded 
#   because they were uploaded before (originals + repeats upload together)

#+ get iTunes-repeats --------------------------------------------------
itunes_repeat_slots <- cz_week %>%
  filter(cz_slot_key == paste0("product ", cycle) & cz_slot_value == "h") %>%
  select(cz_slot_pfx)

#+... join on 'titel', exclude on itunes-repeat ----
cz_week_titles <- cz_week %>% 
  filter(cz_slot_key == "titel") %>% 
  anti_join(itunes_repeat_slots)

#+ get sizes of each program ----
cz_week_sizes <- cz_week %>% 
  filter(cz_slot_key == "size") %>% 
  select(cz_slot_pfx, size = cz_slot_value)

# #+ get regular repeats ----
# suppressWarnings(
#   cz_week_repeats <- cz_week %>%
#     filter(cz_slot_key == "herhaling") %>%
#     mutate(
#       hh_offset_dag = if_else(cz_slot_value == "tw", 
#                               7L, 
#                               as.integer(str_sub(cz_slot_value, 1, 2))),
#       hh_offset_uur = if_else(cz_slot_value == "tw", 
#                               as.integer(str_sub(cz_slot_pfx, 5, 6)),
#                               as.integer(str_sub(cz_slot_value, 5, 6)))
#     ) %>%
#     select(cz_slot_pfx, hh_offset_dag, hh_offset_uur) 
# )

# + join the lot ----
broadcasts.I <- cz_slot_dates %>% 
  inner_join(cz_week_titles) %>% 
  inner_join(cz_week_sizes) %>% 
  # left_join(cz_week_repeats) %>% 
  # inner_join(tbl_gidsinfo, by = c("cz_slot_value" = "key_modelrooster")) %>% 
  # left_join(tbl_gidsinfo_nl_en, by = c("productie_taak" = "item_NL")) %>% 
  # rename(productie_taak_EN = item_EN) %>% 
  # left_join(tbl_gidsinfo_nl_en, by = c("genre_NL1" = "item_NL")) %>% 
  # rename(genre_EN1 = item_EN) %>% 
  # left_join(tbl_gidsinfo_nl_en, by = c("genre_NL2" = "item_NL")) %>% 
  # rename(genre_EN2 = item_EN) %>% 
  # mutate(json_start = date_time,
  #        json_stop = date_time + dminutes(as.integer(size))) %>% 
  select(date_time,
         json_start,
         json_stop,
         hh_offset_dag,
         hh_offset_uur,
         size,
         titel_NL,
         titel_EN,
         genre_NL1,
         genre_EN1,
         genre_NL2,
         genre_EN2,
         intro_NL,
         intro_EN,
         productie_taak,
         productie_taak_EN,
         productie_mdw
  )

broadcasts_orig <- broadcasts.I %>% 
  select(-hh_offset_dag, -hh_offset_uur, -size)

