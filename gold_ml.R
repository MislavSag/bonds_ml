library(jsonlite)
library(data.table)
library(janitor)
library(forecast)
library(lubridate)



# IMPORT DATA -------------------------------------------------------------
# import commodity prices
commodity_prices = fread("data/commodity_prices.csv")
commodity_prices = clean_names(commodity_prices)
commodity_prices[, month := as.Date(paste(gsub("M", "", month), "01"), format = "%Y%m%d")]

# wide to long
dt = melt(commodity_prices, id.vars = "month", variable.name = "var")


# TARGETS -----------------------------------------------------------------
# calculate future returns
dt[, target_ret_1 := shift(value, -1, type = "shift") / value, by = var]
dt[, target_ret_3 := shift(value, -3, type = "shift") / value, by = var]
dt[, target_ret_6 := shift(value, -6, type = "shift") / value, by = var]
dt[, target_ret_12 := shift(value, -12, type = "shift") / value, by = var]


# MACRO DATA --------------------------------------------------------------
# import fred series
fred_dt = fread("C:/macro/fred.csv")

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
fred_dt = fred_dt[date >= dt[, min(month)]]
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
dt = merge(dt, fred_dt_sample, by.x = "month", by.y = "date", all.x = TRUE, all.y = FALSE)

# momentum predictors
setorder(dt, var, month)
mom_width = 1:12
mom_cols = paste0("m_", mom_width)
dt[, (mom_cols) := lapply(mom_width, function(x) value / shift(value, x) - 1), by = var]


dt[, unique(var)]
