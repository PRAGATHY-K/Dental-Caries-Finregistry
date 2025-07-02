##  Purpose  : Extract 2012-2017 births from THL Birth Register and
##             save compressed cohort file (studypop.rds).
##  Inputs   : RAW_ROOT/thl_birth/thl_2021_1222_birth.csv.gz
##  Outputs  : Data_Wrk/studypop.rds   
##  Author   : Pragathy Kannan
##  Updated  : 2025-07-01
## ────────────────────────────────────────────────────────────────────

library(arrow)         # streaming large .csv.gz
library(data.table)    # fast data wrangling
library(lubridate)     # date helpers

# ---- 0  Paths & folders --------------------------------------------
RAW_ROOT  <- Sys.getenv("RAW_DATA_PATH")             # e.g. /media/volume/tmp_data/FinRegistry_2024Q1
DATA_WORK <- Sys.getenv("DATA_WORK")                 # e.g. ~/projects/dental_caries/data_work
dir.create(DATA_WORK, showWarnings = FALSE, recursive = TRUE)

birth_file <- file.path(RAW_ROOT, "thl_birth", "thl_2021_1222_birth.csv.gz")

# ---- 1  Stream register & filter years -----------------------------
ds_birth <- open_dataset(birth_file, format = "csv", delimiter = ";")

birth <- ds_birth |>
  mutate(birth_date = ymd(LAPSEN_SYNTYMAPVM)) |>          # convert to Date
  filter(birth_date >= ymd("2012-01-01"),                  # keep 2012-2017
         birth_date <  ymd("2018-01-01")) |>
  select(
    FINREGISTRYID      = LAPSI_FINREGISTRYID,              # child ID
    birth_date,
    sex                = SUKUP,                            # 1=boy 2=girl
    gest_age_days      = KESTOVKPV,                        # gestation days
    birth_weight_sd    = SYNTYMAPAINO_SD,                  # weight z-score
    maternal_age       = AITI_IKA,
    income_quintile    = INCOME_QUINT                      # SES proxy
  ) |>
  collect()                                                # into RAM (320k)

# ---- 2  Save compressed cohort -------------------------------------
saveRDS(birth,
        file.path(DATA_WORK, "studypop.rds"),
        compress = "xz")

cat("✓  studypop.rds saved with", nrow(birth), "rows\n")
