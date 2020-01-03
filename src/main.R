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
mtr_max <- tbl_montage %>% 
  arrange(desc(Uitzending)) %>% 
  filter(row_number() == 1L) %>% 
  select(Uitzending)

mtr_window <- mtr_max %>% 
  mutate(run_start = if_else(wday(Uitzending, label = T, abbr = T) == "do", # legend: 1 = zo
                                  Uitzending,
                                  Uitzending + ddays(1L)),
         run_stop = run_start + ddays(7L)
  )
  

# retrieve all info like it is done for "P1-wpgidsweek". Sharing this at code-level
# will be a future excercise :)
# connect new to existing code base variables
current_run_start <- mtr_window$run_start
flog.info("Dit wordt de MT-roosterweek van %s", 
          format(current_run_start, "%A %d %B %Y"),
          name = "mtr_log")

# cz-week's 168 hours comprise 8 weekdays, not 7 (Thursday AM and PM)
# but to the schedule-template both Thursdays are the same, as the
# template is undated.
# Both Thursday parts will separate when the schedule gets 'calendarized'
current_run_stop <- mtr_window$run_stop

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
week_0.I <- tbl_zenderschema %>%
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
week_0.II <- gather(week_0.I, slot_key, slot_value, -cz_slot, na.rm = T) %>% 
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
week_0.III <-
  gather(
    data = week_0.II,
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
week_temp <- week_0.III %>% 
  select(-cz_slot_key, -cz_slot_value) %>% 
  mutate(cz_slot_key = "size",
         cz_slot_key = factor(x = cz_slot_key, ordered = T, levels = cz_slot_key_levels),
         cz_slot_value = cz_slot_size) %>% 
  select(cz_slot_day, ord_day, cz_slot_hour, cz_slot_key, cz_slot_value, cz_slot_size) %>% 
  distinct

# + final result week-0 ---------------------------------------------------
week_0 <- bind_rows(week_0.III, week_temp) %>% 
  select(-cz_slot_size) %>% 
  arrange(cz_slot_day, ord_day, cz_slot_hour, cz_slot_key)

rm(week_0.I, week_0.II, week_0.III, week_temp)

# = = = = = = = = = = = = = = = = = = = = = = = = + = = = = = = = = = -----

# prep week-1/5 (days 1-7/8-14/... of month) ------------------------------
# no matter which weekday comes first!

source("src/mt_rooster_shared_functions.R")

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
cycle <- get_cycle(as.Date(current_run_start))

# + join the lot ----
# !deprecated! set the product belonging to the other cycle, so it can be skipped
# opposing_product <- paste0("product ", if_else(cycle == "A", "B", "A"))
broadcasts.I <- cz_slot_dates %>% 
  inner_join(cz_week) %>% 
  filter(cz_slot_key != "herhaling") # %>% 
  # filter(cz_slot_key != opposing_product)

#+ pivot-wide the df ----
broadcasts.II <- broadcasts.I %>% 
  spread(key = cz_slot_key, value = cz_slot_value) 

#+ skip mtr-flag = F ----
# keep only those with mtr-flag; format to match spreadsheet
broadcasts.III <- broadcasts.II %>%
  filter(is.na(`in mt-rooster`) | `in mt-rooster` != "n") %>% 
  mutate(sh_dockrefs_m2p = 0,
         sh_uzd = as.integer(format(date_time, "%Y%m%d")),
         sh_kleur = (row_number() %% 2L) 
                  + banding_offset(date(date_time)),
         sh_kleur = if_else(is.na(balk), sh_kleur, 4L),
         sh_uitzending = date(date_time),
         sh_tijd.duur = as.integer(paste0(str_sub(cz_slot_pfx, 5), size)),
         sh_gids = NA_character_,
         sh_titel = titel,
         sh_type = NA_character_,
         sh_presentatie = NA_character_,
         sh_techniek = NA_character_,
         sh_datum = NA,
         sh_bijzonderheden = as.integer(`cmt mt-rooster`),
         sh_redactie = NA_character_,
         sh_extra = NA_character_) %>% 
  select(starts_with("sh_"), 
         starts_with("prod"),
         balk)

#+ set product ----
# formerly known as "broadcast type"
if (cycle == "A") {
  broadcasts.IV <- broadcasts.III %>% 
    mutate(sh_type = paste0(if_else(is.na(product), 
                                    "", 
                                    product), 
                            if_else(is.na(`product A`),
                                    "",
                                    `product A`)
                            )
    )
} else {
  broadcasts.IV <- broadcasts.III %>% 
    mutate(sh_type = paste0(if_else(is.na(product), 
                                    "", 
                                    product), 
                            if_else(is.na(`product B`),
                                    "",
                                    `product B`)
                            )
    )
}

#+ replace "balk", trim select list ----
broadcasts.V <- broadcasts.IV %>% 
  mutate(sh_titel = if_else(is.na(balk), sh_titel, balk)) %>% 
  select(-starts_with("prod"), -balk)

#+ dereference bijz.mtr ----
broadcasts <- broadcasts.V %>% 
  left_join(tbl_bijzonderheden, by = c("sh_bijzonderheden" = "verwijzing")) %>% 
  mutate(sh_bijzonderheden = bijzonderheden,
         sh_presentatie = if_else(sh_type == "h", "x", NA_character_),
         sh_techniek = if_else(sh_type == "h", "x", NA_character_)
  ) %>% 
  select(starts_with("sh_"))

rm(broadcasts.I, broadcasts.II, broadcasts.III, broadcasts.IV, broadcasts.V)

# save mtr-week details as tsv ----
mtr_file_local <- paste0(config$output.dir, "mtr.tsv")

write.table(x = broadcasts,
            file = mtr_file_local,
            append = F,
            quote = F,
            sep = "\t",
            eol = "\n",
            na = "",
            row.names = F,
            col.names = F,
            fileEncoding = "UTF-8")

# upload to GD ----
# mtr_file_gd <- 

# drive_rm(mtr_file_gd)
#> Files deleted:
#>   * README-chicken.csv: 1mjn-J_HbyfQisV3Kpl__C5IBLFiGW-1X

mtr_sheet <- drive_upload(media = mtr_file_local,
                          name = "mtr_nieuwe_week",
                          type = "spreadsheet",
                          overwrite = T
)

#> Local file:
#>   * /Users/jenny/Library/R/3.6/library/googledrive/extdata/chicken.csv
#> uploaded into Drive file:
#>   * README-chicken-sheet: 1j2VsF1NcYlc6W9OwenhhMijl7u7HOxpdDXY9UJrg_SM
#> with MIME type:
#>   * application/vnd.google-apps.spreadsheet