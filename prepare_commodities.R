library(jsonlite)
library(data.table)
library(janitor)
library(forecast)
library(lubridate)
library(glue)
library(fs)
library(findata)
library(lubridate)
library(forecast)
library(readxl)
library(rvest)
library(httr)
library(ggplot2)


# PARAMETERS --------------------------------------------------------------
FREQ   = "month" # can be week or month
# start date, can be one of
# fread(glue("data/fred_col_month.csv"))[, unique(start_date)]
STARTD = as.Date("1961-06-01")


# IMPORT DATA -------------------------------------------------------------
# Download and import monthly commodity prices from World Bank
url = "https://www.worldbank.org/en/research/commodity-markets"
xlsx_url = rvest::read_html(url) |>
  html_elements("a") |>
  html_attr("href") |>
  grep("Monthly", x = _, value = TRUE)
xlsx_file = "data/commodity_prices.xlsx"
GET(xlsx_url, write_disk(xlsx_file, overwrite = TRUE))
commodity_prices = read_xlsx(xlsx_file, sheet = "Monthly Prices", skip = 4)
setDT(commodity_prices)
commodity_prices = commodity_prices[-1]
colnames(commodity_prices)[1] = "month"
commodity_prices = clean_names(commodity_prices)
commodity_prices[, month := as.Date(paste(gsub("M", "", month), "01"), format = "%Y%m%d")]
cols_char = colnames(commodity_prices)[2:ncol(commodity_prices)]
commodity_prices[, (cols_char) := lapply(.SD, as.numeric), .SDcols = cols_char]

# wide to long
dt = melt(commodity_prices, id.vars = "month", variable.name = "var")


# TARGETS -----------------------------------------------------------------
# calculate future returns
dt[, target_ret_1 := shift(value, -1, type = "shift") / value - 1, by = var]
# dt[, target_ret_3 := shift(value, -3, type = "shift") / value - 1, by = var]
# dt[, target_ret_6 := shift(value, -6, type = "shift") / value - 1, by = var]
# dt[, target_ret_12 := shift(value, -12, type = "shift") / value - 1, by = var]


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

# predictors FRED
predictors_fred = colnames(fred_dt)[2:ncol(fred_dt)]


# PREDICTORS --------------------------------------------------------------
# Merge yields and macro data
dt = merge(dt, fred_dt, by.x = "month", by.y = "date", all.x = TRUE, all.y = FALSE)

# Momentum predictors
setorder(dt, var, month)
mom_width = 1:12
mom_cols = paste0("m_", mom_width)
dt[, (mom_cols) := lapply(mom_width, function(x) value / shift(value, x) - 1), by = var]

# Plot few commodity series
ggplot(dt[var %in% c("silver", "crude_oil_average"), .(month, value, var)][200:150],
       aes(x = month, y = value, color = var)) +
  geom_line() +
  theme_minimal()

# save
fwrite(dt, "data/commodities_dt.csv")
