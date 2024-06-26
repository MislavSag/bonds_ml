library(data.table)
library(lubridate)
library(forecast)
library(findata)
library(AzureStor)
library(findata)
library(fs)
library(glue)
library(stringr)
library(janitor)
library(MultiATSM)


# PARAMETERS --------------------------------------------------------------
FREQ   = "month" # can be week or month
# start date, can be one of
# fread(glue("data/fred_col_month.csv"))[, unique(start_date)]
STARTD = as.Date("1961-06-01")


# YIELDS DATA -------------------------------------------------------------
# help function to convert yields to MultiATSM format
yields_prepare = function(dt_, remove = c("label", "dates")) {
  dt_ = dt_[, tail(.SD, 1), by = .(label, month)]
  if (remove == "label") {
    dt_[, remove := any(is.na(yield)), by = label]
    dt_ = dt_[remove == FALSE]
  }
  dt_ = dcast(dt_, label ~ month, value.var = "yield")
  dt_
}
yields_gen = Yields$new(countries = c("US"))
yields_raw = yields_gen$get_yields()
yields_raw[, month := as.IDate(ceiling_date(date, "month"))]
yields = yields_raw[, tail(.SD, 1), by = .(label, month)]

# Remove columns we dont need
yields[, date := NULL]

# Change label to be the same as in lw data
yields[, label := paste0("m", gsub("Y|M_US", "", label))]

# Sort
setnames(yields, c("month", "label", "yield"),
         c("date", "maturity", "yield_treasury"))
setorder(yields, maturity, date)

# Check last date
yields[date == max(date)]

# # Function to modify the Google Drive URL for direct download
# modify_google_drive_url <- function(url) {
#   id <- sub(".*id=([^&]*).*", "\\1", url)
#   return(paste0("https://drive.google.com/uc?export=download&id=", id))
# }

# Import Lui and Wu data
# source: https://sites.google.com/view/jingcynthiawu/yield-data
lw = fread("data/lw_monthly.csv")
names_ = str_extract(colnames(lw), "\\d+")[-1]
names_ = paste0("m", names_)
setnames(lw, c("date", names_))
lw[, date := as.Date(paste0(as.character(date), "01"), format = "%Y%m%d")]
lw[, date]
lw[, date := as.IDate(ceiling_date(date, "month"))] # IMPORTANT
lw = melt(lw,
          id.vars = "date",
          variable.name = "maturity",
          value.name = "yield",
          variable.factor = FALSE)

# Merge old and new data
maturities_keep = yields[, unique(maturity)]
yieldsadj = lw[maturity %in% maturities_keep]
yieldsadj = merge(yieldsadj, yields, by = c("maturity", "date"), all = TRUE)
lw_max_date = lw[, max(date)]
next_date = lw_max_date %m+% months(1)
yieldsadj[, ratio := .SD[date == next_date, yield_treasury] / .SD[date == lw_max_date, yield],
          by = maturity]
yieldsadj[, yield := yield * ratio]
yieldsadj[, yield := ifelse(is.na(yield), yield_treasury, yield)]
yieldsadj[, `:=`(yield_treasury = NULL, ratio = NULL)]

# Checks
# yieldsadj[, unique(maturity)]
# yieldsadj[m == "m1"]
# lw[maturity == "m1"]


# PRICES, ECESS RETURNS AND FORWARDS --------------------------------------
# calculate bond prices
yieldsadj[, maturity_months := as.integer(sub("m", "", maturity))]
yieldsadj[, maturity_years := maturity_months / 12]
yieldsadj[, price := exp(-maturity_years * (yield / 100))]
yieldsadj[, price_log := log(price)]

# calculate excess returns
excess_returns = function(m = 1) {
  # m = 1
  setorder(yieldsadj, maturity_months, date)
  yieldsadj[, excess_return := shift(price_log, -m, type = "shift") - price_log, by = maturity]
  setnames(yieldsadj, "excess_return", paste0("excess_return", "_", m))
  yieldsadj
}
excess_returns(1)
excess_returns(3)
excess_returns(6)
excess_returns(12)

# Checks
yieldsadj[is.na(excess_return_1 & maturity_months < 120)]


# MACRO DATA --------------------------------------------------------------
# Import data we need
fred_columns = fread(glue("data/fred_col_month.csv"))
fred_columns[, unique(start_date)]
fred_columns = fred_columns[start_date == STARTD, cols]

# Import fresh data
Sys.setenv("FRED-KEY" = "fb7e8cbac4b84762980f507906176c3c")
temp_dir = tempdir()
if (fs::dir_exists(temp_dir)) {
  dir_delete(temp_dir)
  dir_create(temp_dir)
}
fred = MacroData$new(temp_dir)
fred$bulk_fred(fred_columns)
files_ = dir_ls(path(temp_dir, "fred"))
fred_dt = lapply(files_, fread)
fred_dt = lapply(fred_dt, function(dt_) dt_[, value := as.numeric(value)])
fred_dt = rbindlist(fred_dt)
fred_dt[, date_real := date]
fred_dt[vintage == 1, date_real := realtime_start]
fred_dt[, realtime_start := NULL]
fred_dt[, max(date_real)]
fred_dt = unique(fred_dt, by = c("series_id", "date_real"))

ceiling_biweek <- function(date) {
  weeks_since_epoch <- floor(as.numeric(difftime(date, as.Date('1970-01-05'), units = "weeks")) / 2)
  next_biweek_start <- as.Date('1970-01-05') + weeks(weeks_since_epoch * 2 + 2)
  return(next_biweek_start)
}

if (FREQ == "biweek") {
  fred_dt[, biweek := ceiling_biweek(date_real)]
} else if (FREQ == "month") {
  fred_dt[, month := ceiling_date(date_real, "month")]
}
setorderv(fred_dt, c("series_id", FREQ))
fred_dt = fred_dt[, last(.SD), by = c("series_id", FREQ)]
cols_keep = c("series_id", FREQ, "value")
fred_dt = fred_dt[, ..cols_keep]
fred_dt = fred_dt[x > as.Date("1961-05-01"), env = list(x = FREQ)]
setorderv(fred_dt, c("series_id", FREQ))

# Diff if necessary
fred_dt = fred_dt[, ndif := ndiffs(value), by = series_id]
fred_dt[, unique(ndif), by = series_id][, .N, by = V1]
fred_dt[ndif > 0, value_diff := c(rep(NA, unique(ndif)), diff(value, unique(ndif))), by = series_id]

# Reshape
if (FREQ == "month") {
  fred_dt = dcast(fred_dt[, .(month, series_id, value)],
                  month ~ series_id,
                  value.var = "value")
} else if (FREQ == "biweek") {
  fred_dt = dcast(fred_dt[, .(biweek, series_id, value)],
                  biweek ~ series_id,
                  value.var = "value")
}

# Change column name to be the same as in other objects
setnames(fred_dt, FREQ, "date")

# Fill by nalocf
setnafill(fred_dt, "locf")

# Remove missing vlaues
fred_dt = na.omit(fred_dt)
setorder(fred_dt, date)

# predictors fred
predictors_fred = colnames(fred_dt)[2:ncol(fred_dt)]


# PREDICTORS --------------------------------------------------------------
# Merge yields and macro data
dt = merge(fred_dt, yieldsadj, by = "date", all.x = TRUE, all.y = FALSE)

# Checks
dt[, max(date)]
dt[is.na(excess_return_1 & maturity_months < 120)]

# momentum predictors
setorder(dt, maturity, date)
if (FREQ == "biweek") {
  mom_width = 1:26
} else if (FREQ == "month") {
  mom_width = c(1:12)
}
mom_cols = paste0("m_", mom_width)
head(dt[, .(date, price)], 50)
dt[, (mom_cols) := lapply(mom_width, function(x) price / shift(price, x) - 1),
   by = maturity]

# yield curve factors
yields_multiatsm = dt[, .(date, maturity, yield)]
yields_multiatsm = dcast(yields_multiatsm, maturity ~ date, value.var = "yield")
yields_multiatsm[, month_ := as.integer(gsub("m", "", maturity))]
yields_multiatsm[, maturity := paste0("Y", month_, "M_US")]
setorder(yields_multiatsm, month_)
yields_multiatsm = as.data.frame(yields_multiatsm)
yields_multiatsm = remove_empty(yields_multiatsm, which = "cols")
rownames(yields_multiatsm) = yields_multiatsm[, 1]
yields_multiatsm = yields_multiatsm[, -1]
yields_multiatsm = na.omit(yields_multiatsm)
yields_multiatsm$month_ = NULL
yields_multiatsm = as.matrix(yields_multiatsm)
yields_multiatsm[1:nrow(yields_multiatsm), 1:5]
spa_fact = MultiATSM::Spanned_Factors(yields_multiatsm, "US", 3)
spa_fact = as.data.table(spa_fact, keep.rownames = "var")
spa_fact = melt(spa_fact, id.vars = "var", variable.name = "date")
spa_fact = dcast(spa_fact, date ~ var)
setnames(spa_fact, colnames(spa_fact), gsub(" ", "_", colnames(spa_fact)))
spa_fact[, date := as.IDate(date)]
dt = merge(dt, spa_fact, by = "date", all.x = TRUE)

# Checks
dt[, min(date)]
dt[, max(date)]
dt[1:10]
dt[is.na(excess_return_1) & maturity_months < 120]

# save to Azure
endpoint = "https://snpmarketdata.blob.core.windows.net/"
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
cont = storage_container(BLOBENDPOINT, "padobran")
time_ = strftime(Sys.time(), format = "%Y%m%d")
file_name = glue("bonds-predictors-{FREQ}-{time_}.csv")
print(file_name)
fwrite(dt, path("data", file_name))
storage_write_csv(dt, cont, file_name)

# Move to padobran if necessary (if want to backtest on all data)
paste0("scp ", getwd(), path("/data", file_name), " padobran:/home/jmaric/bonds_ml/", file_name)
# "scp /home/sn/data/strategies/pread/dataset_pread.csv padobran:/home/jmaric/pread/dataset_pread.csv"
