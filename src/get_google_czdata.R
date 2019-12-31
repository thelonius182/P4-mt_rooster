library(googledrive)
library(keyring)
library(readxl)
library(yaml)

cz_extract_sheet <- function(ss_name, sheet_name) {
  read_xlsx(ss_name,
            sheet = sheet_name,
            .name_repair = ~ ifelse(nzchar(.x), .x, LETTERS[seq_along(.x)]))
}

cz_get_url <- function(cz_ss) {
  cz_url <- paste0("url_", cz_ss)
  paste0("https://", config$url_pfx, config[[cz_url]])
}

# downloads GD ----
# aanmelden bij GD loopt via de procedure die beschreven is in "Operation Missa > Voortgang > 4.Toegangsrechten GD".

# Nipper-spreadsheet ophalen bij GD
path_roosters <- paste0(config$gs_downloads, "/", "roosters.xlsx")
gd_file <- cz_get_url("roosters")
drive_download(file = gd_file, overwrite = T, path = path_roosters)

# sheets als df ----
tbl_montage <- cz_extract_sheet(path_roosters, sheet_name = "montage")
