library(data.table)
library(lubridate)
library(forecast)
library(findata)
library(AzureStor)



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

# remove columns we dont need
yields[, date := NULL]

# change label to be the same as in lw data
yields[, label := paste0("m", gsub("Y|M_US", "", label))]

# sort
setnames(yields, c("month", "label", "yield"),
         c("date", "maturity", "yield_treasury"))
setorder(yields, maturity, date)

# check last date
yields[date == max(date)]

# Function to modify the Google Drive URL for direct download
modify_google_drive_url <- function(url) {
  id <- sub(".*id=([^&]*).*", "\\1", url)
  return(paste0("https://drive.google.com/uc?export=download&id=", id))
}

# Import Lui and Wu data
lw = fread("data/lw_monthly.csv")
lw = melt(lw, id.vars = "date",
          variable.name = "maturity",
          value.name = "yield",
          variable.factor = FALSE)

# merge old and new data
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


# PRICES, ECESS RETURNS AND FORWARDS --------------------------------------
# calculate bond prices
yieldsadj[, maturity_months := as.integer(sub("m", "", maturity))]
yieldsadj[, maturity_years := maturity_months / 12]
yieldsadj[, price := exp(-maturity_years * (yield / 100))]
yieldsadj[, price_log := log(price)]

# calculate excess returns
excess_returns = function(m = 1) {
  setorder(yieldsadj, maturity_months, date)
  yieldsadj[, excess_return := shift(price_log, -m, type = "shift") - price_log, by = maturity]
  setnames(yieldsadj, "excess_return", paste0("excess_return", "_", m))
  yieldsadj
}
excess_returns(1)
excess_returns(3)
excess_returns(6)
excess_returns(12)


# MACRO DATA --------------------------------------------------------------
# import fred series
fred_dt = fread("F:/macro/fred.csv")

# remove unnecessary columns
fred_dt = fred_dt[, .(series_id, date = date_real, frequency_short, popularity,
                      units, id_category, value)]

# diff if necessary
fred_dt = fred_dt[, ndif := ndiffs(value), by = series_id]
fred_dt[, unique(ndif), by = series_id][, .N, by = V1]
fred_dt[ndif > 0, value_diff := c(rep(NA, unique(ndif)), diff(value, unique(ndif))), by = series_id]

# check for duplicates
date_series = fred_dt[, .N, by = .(date, series_id)]
fred_dt = unique(fred_dt, fromLast = TRUE, by = c("series_id", "date"))
rm(date_series)

# downsample to monthly frequency
fred_dt[, month := ceiling_date(date, "month")]
fred_dt = fred_dt[, tail(.SD, 1), by = c("series_id", "month")]

# reshape
fred_dt = dcast(fred_dt[, .(month, series_id, value_diff)],
                month ~ series_id,
                value.var = "value_diff")
setnames(fred_dt, "month", "date")
setorder(fred_dt, date)
fred_dt[1:10, 1:10]

# remove predictors that doesnt have data for last year
na_check_last_year = colSums(is.na(tail(fred_dt, 12)))
fred_dt = fred_dt[, .SD, .SDcols = na_check_last_year < 12]

# locf vars
predictors = colnames(fred_dt)[2:ncol(fred_dt)]
fred_dt[, (predictors) := lapply(.SD, nafill, type = "locf"), .SDcols = predictors]

# lag all variables - IMPORTANT !
# fred_dt[, (predictors) := lapply(.SD, shift), .SDcols = predictors]

# filter dates
fred_dt = fred_dt[date >= lw[, min(date)]]
fred_dt[1:10, 1:10]

# check for missing values
na_check = colSums(is.na(fred_dt))
sum(na_check == 0)
fred_dt[, .SD, .SDcols = na_check == 0]
fred_dt_sample = fred_dt[, .SD, .SDcols = na_check == 0]

# predictors fred
predictors_fred = colnames(fred_dt_sample)[2:ncol(fred_dt_sample)]


# PREDICTORS --------------------------------------------------------------
# merge yields and macro data
dt = merge(yieldsadj, fred_dt_sample, by = "date", all.x = TRUE, all.y = FALSE)

# momentum predictors
setorder(dt, maturity, date)
mom_width = 1:12
mom_cols = paste0("m_", mom_width)
dt[, (mom_cols) := lapply(mom_width, function(x) price / shift(price, x) - 1), by = maturity]

# yield curve factors
yields_multiatsm = dt[, .(date, maturity, yield)]
yields_multiatsm = dcast(yields_multiatsm, maturity ~ date)
yields_multiatsm[, month_ := as.integer(gsub("m", "", maturity))]
yields_multiatsm[, maturity := paste0("Y", month_, "M_US")]
setorder(yields_multiatsm, month_)
yields_multiatsm = as.data.frame(yields_multiatsm)
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

# save to Azure
endpoint = "https://snpmarketdata.blob.core.windows.net/"
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
cont = storage_container(BLOBENDPOINT, "padobran")
storage_write_csv(dt, cont, "bonds-predictors.csv")
