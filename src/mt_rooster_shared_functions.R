prep_week <- function(week_nr) {
  week_x_init <- tbl_zenderschema %>%
    select(cz_slot = slot, contains(week_nr))
  
  # pivot-long to collapse week attributes into NV-pairs ------------------
  week_x_long <-
    gather(
      data = week_x_init,
      key = slot_key,
      value = slot_value, -cz_slot,
      na.rm = T
    ) %>%
    mutate(
      slot_key = case_when(
        slot_key == paste0("week_", week_nr) ~ "titel",
        slot_key == paste0("bijzonderheden_week_", week_nr) ~ "cmt mt-rooster",
        slot_key == paste0("product_week_", week_nr) ~ "product",
        T ~ slot_key
      ),
      slot_key = factor(slot_key, ordered = T, levels = cz_slot_key_levels),
      cz_slot_day = str_sub(cz_slot, 1, 2),
      cz_slot_day = factor(cz_slot_day, ordered = T, levels = weekday_levels),
      cz_slot_hour = str_sub(cz_slot, 3, 4),
      cz_slot_size = str_sub(cz_slot, 5),
      ord_day = as.integer(week_nr)
    ) %>%
    select(
      cz_slot_day,
      ord_day,
      cz_slot_hour,
      cz_slot_key = slot_key,
      cz_slot_value = slot_value,
      cz_slot_size
    ) %>%
    arrange(cz_slot_day, cz_slot_hour, cz_slot_key)
  
  # promote slot-size to be an attribute too ------------------------------
  week_temp <- week_x_long %>%
    select(-cz_slot_key, -cz_slot_value) %>%
    mutate(
      cz_slot_key = "size",
      cz_slot_key = factor(x = cz_slot_key,
                           ordered = T,
                           levels = cz_slot_key_levels),
      cz_slot_value = cz_slot_size
    ) %>%
    select(cz_slot_day,
           ord_day,
           cz_slot_hour,
           cz_slot_key,
           cz_slot_value,
           cz_slot_size) %>%
    distinct
  
  # final result week-x ---------------------------------------------------
  week_x <- bind_rows(week_x_long, week_temp) %>%
    select(-cz_slot_size) %>%
    arrange(cz_slot_day, ord_day, cz_slot_hour, cz_slot_key)
  
  rm(week_x_init, week_x_long, week_temp)
  
  return(week_x)
}

get_cycle <- function(cz_week_start) {
  # test: cz_week_start <- ymd("2019-11-21")
  ref_date_B_cycle <- ymd("2019-10-17")
  i_diff <- as.integer(cz_week_start - ref_date_B_cycle) %/% 7L
  if_else(i_diff %% 2 == 0, "B", "A")
}

fmt_utc_ts <- function(some_date) {
  format(some_date, "%Y-%m-%d_%a%H-%Z%z") %>%
    str_replace("CES?T", "UTC") %>%
    str_sub(1, 22)
}

