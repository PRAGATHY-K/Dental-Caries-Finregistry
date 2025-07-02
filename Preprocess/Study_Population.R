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



----------------------------------------------------------------------

##  Script   : 02_add_death_censor.R
##  Purpose  : Merge Central Death Register to birth cohort and create
##             per-child censoring date (min[birth+6 y, death]).
##  Inputs   :   • data_work/studypop.rds        (built in 01 script)
##               • RAW_ROOT/sf_death/sf_death_2023.csv.gz
##  Outputs  : data_work/cohort_censored.rds
##  Author   : Pragathy Kannan
##  Updated  : 2025-07-01
## ────────────────────────────────────────────────────────────────────
library(data.table)
library(lubridate)

# ---- 0  Paths ---------------------------------------------------------------
RAW_ROOT  <- Sys.getenv("RAW_DATA_PATH")          # /media/volume/tmp_data/FinRegistry_2024Q1
DATA_WORK <- Sys.getenv("DATA_WORK")              # ~/projects/dental_caries/data_work
dir.create(DATA_WORK, showWarnings = FALSE, recursive = TRUE)

birth_file  <- file.path(DATA_WORK, "studypop.rds")
death_file  <- file.path(RAW_ROOT, "sf_death", "sf_death_2023.csv.gz")

# ---- 1  Read birth cohort ---------------------------------------------------
birth <- readRDS(birth_file)          # 320 k rows, already a data.frame
setDT(birth)

# ---- 2  Read & tidy death register -----------------------------------------
## Death file stores only YEAR (KVUOSI).  Assume death occurred
## on 31-Dec of that year – conservative censoring.
death <- fread(
  death_file,
  sep    = ";",
  select = c("TNRO", "KVUOSI")        # TNRO = FINREGISTRYID, KVUOSI = year
)[
  , .(
      FINREGISTRYID = as.character(TNRO),
      death_date    = ymd(sprintf("%d-12-31", KVUOSI))
  )
]

# ---- 3  Merge & create censoring date --------------------------------------
birth[ , FINREGISTRYID := as.character(FINREGISTRYID)]  # unify type

cohort_cens <- merge(
  birth, death,
  by   = "FINREGISTRYID",
  all.x = TRUE                 # keep every child (NA death = alive)
)

## Follow-up ends at earlier of 6th birthday OR death
cohort_cens[ ,
  fu_end := pmin(birth_date + years(6), death_date, na.rm = TRUE)
]

# ---- 4  Save ---------------------------------------------------------------
saveRDS(cohort_cens,
        file.path(DATA_WORK, "cohort_censored.rds"),
        compress = "xz")





-------------------------------------------------------------------------------
## ────────────────────────────────────────────────────────────────────
##  Script   : 03_add_ecc_event.R
##  Purpose  : Stream detailed_longitudinal register once to find the
##             earliest ICD-10 K02 (dental caries) per child and merge
##             into censored cohort.
##  Inputs   :   • Data_wrk/cohort_censored.rds
##               • RAW_ROOT/detailed_longitudinal.csv.gz
##  Outputs  : data_work/cohort_ecc.rds
##  Author   : Pragathy Kannan
Updated  : 2025-07-01
## ────────────────────────────────────────────────────────────────────
library(data.table)
library(lubridate)

RAW_ROOT  <- Sys.getenv("RAW_DATA_PATH")
DATA_WORK <- Sys.getenv("DATA_WORK")

cohort_cens <- readRDS(file.path(DATA_WORK, "cohort_censored.rds"))
setDT(cohort_cens)[ , FINREGISTRYID := as.character(FINREGISTRYID)]

dl_file <- file.path(RAW_ROOT, "detailed_longitudinal.csv.gz")

# ---- 1  Detect column positions (header only) ------------------------------
hdr   <- readLines(pipe(sprintf("gzip -dc %s | head -n 1", shQuote(dl_file))), 1)
cols  <- strsplit(hdr, ",")[[1]]
pos   <- match(tolower(
                 c("finregistryid","event_day","source","code1")),
               tolower(cols))
if (anyNA(pos))
  stop("Required columns not found in detailed_longitudinal header.")

# ---- 2  Stream only those 4 columns via gzip + cut -------------------------
cmd <- sprintf("gzip -dc %s | cut -d',' -f%s",
               shQuote(dl_file), paste(pos, collapse = ","))

dl_small <- fread(
  cmd = cmd,
  sep = ",",
  col.names = c("FINREGISTRYID","event_day","source","code1"),
  showProgress = TRUE
)

# ---- 3  Earliest primary-care K02 per child ---------------------------------
ecc <- dl_small[
          source == "PRIM_OUT" & grepl("^K02", code1),
          .(ecc_date = min(ymd(event_day))),
          by = FINREGISTRYID
       ]

saveRDS(ecc,
        file.path(DATA_WORK, "ecc_icd_only.rds"),
        compress = "xz")

# ---- 4  Merge into cohort & derive analysis fields -------------------------
cohort_ecc <- merge(
  cohort_cens, ecc,
  by = "FINREGISTRYID", all.x = TRUE
)

## 4a age at event (years)
cohort_ecc[ ,
  age_at_ecc := as.numeric((ecc_date - birth_date) / dyears(1))
]

## 4b event indicator and survival time
cohort_ecc[ ,
  ecc_event := !is.na(ecc_date) & age_at_ecc < 6
]

cohort_ecc[ ,
  time_to_event := fifelse(
    ecc_event,
    as.integer(ecc_date - birth_date),   # days to first ECC
    as.integer(fu_end   - birth_date)    # days to censoring
  )
]

# ---- 5  Save final analysis set --------------------------------------------
saveRDS(cohort_ecc,
        file.path(DATA_WORK, "cohort_ecc.rds"),
        compress = "xz")

cat("✓  cohort_ecc.rds saved –", nrow(cohort_ecc),
    "children (ECC events:", sum(cohort_ecc$ecc_event), ")\n")

cat("✓  cohort_censored.rds saved –",
    nrow(cohort_cens), "children\n")
