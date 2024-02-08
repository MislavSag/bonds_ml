library(data.table)
library(lubridate)
library(forecast)
library(fs)


# Filter sries id's with frequency

# Import fred series
fred_dt = fread("F:/data/macro/fred_cleaned.csv")

# remove observations where there is no data 4 months before today
cols_keep = fred_dt[ , .(keep = any(max(date_real) > (Sys.Date()-365/4))), by=series_id]
cols_keep = cols_keep[keep == TRUE, series_id]
fred_dt[, length(unique(series_id))] - length(cols_keep)
fred_dt = fred_dt[series_id %chin% cols_keep]

# downsample to monthly frequency
fred_dt[, month := ceiling_date(date_real, "month")]
fred_dt = fred_dt[, last(.SD), by = c("series_id", "month")]

# select columns we need
fred_dt = fred_dt[, .(series_id, month, value)]

# filter by month
fred_dt = fred_dt[month > as.Date("1961-05-01")]

# add all dates to all series (there is no difference, but for clarity)
dates = fred_dt[month > as.Date("1961-05-01"), unique(month)]
all_ids = fred_dt[, unique(series_id)]
ids = CJ(series_id = all_ids, month = dates, sorted=FALSE)
fred_dt = merge(ids, fred_dt, by = c("series_id", "month"), all.x = TRUE, all.y = FALSE)

# free memory
rm(list = c("ids", "dates", "all_ids", "cols_keep"))

# order by date
setorder(fred_dt, series_id, month)

# locf data
fred_dt[, value := nafill(value, type = "locf"), by = series_id]

# We want to keep only columns that have dates for all observations after some date
dates_start = c(as.Date("1961-06-01"),
                as.Date("1980-01-01"),
                as.Date("1990-01-01"),
                as.Date("2000-01-01"))
columns_save = list()
for (i in seq_along(dates_start)) {
  # filter dates
  d = dates_start[i]

  # keep only series_id which doesnt have missing values
  check_na = fred_dt[month >= d, any(is.na(value)), by = series_id]
  columns_save[[i]] = fred_dt[, .(start_date = d, cols = check_na[V1 == FALSE, series_id])]
}
columns_save = rbindlist(columns_save)

# final dt object with sampled data
fred_sample = fred_dt[series_id %chin% columns_save[, unique(cols)]]

# save final objects
fwrite(columns_save, "data/fred_col.csv")
fwrite(fred_sample, "data/fred_sample.csv")
