library(data.table)
library(lubridate)
library(forecast)
library(fs)


# TODO : move some of following steps to server

# Import fred series
list.files("F:/data/macro/fred")
fred_dt = lapply(dir_ls("F:/data/macro/fred"), fread)
fred_dt = rbindlist(fred_dt)

# Check dupplicates. I just want to check how many duplicates area there
# Otherwise, this is not necessary step. Later when I will move function to
# server to make clean fred_dt I can remove this step.
dup_realtime_start = fred_dt[, .N, by = .(series_id, realtime_start)]
dup_realtime_start[, (sum(N) - nrow(dup_realtime_start)) / nrow(fred_dt)]
duplicates_date = fred_dt[, .N, by = .(series_id, date)]
duplicates_date[, (sum(N) - nrow(duplicates_date)) / nrow(fred_dt)]

# Create date column
fred_dt[, date_real := date]
fred_dt[vintage == 1, date_real := realtime_start]
fred_dt[, realtime_start := NULL]

# keep unique dates by keeping first observation
fred_dt = unique(fred_dt, by = c("series_id", "date_real"))

# remove observations where there is no data 4 months before today
cols_keep = fred_dt[ , .(keep = any(max(date) > (Sys.Date()-365/4))), by=series_id]
cols_keep = cols_keep[keep == TRUE, series_id]
fred_dt = fred_dt[series_id %in% cols_keep]

# # remove unnecessary columns
# fred_dt = fred_dt[, .(series_id, date_real = realtime_start, frequency_short,
#                       popularity, units, id_category, value)]

# downsample to monthly frequency
fred_dt[, month := ceiling_date(date, "month")]
fred_dt = fred_dt[, tail(.SD, 1), by = c("series_id", "month")]

# reshape
fred_dt = dcast(fred_dt[, .(month, series_id, value)],
                month ~ series_id,
                value.var = "value")

# locf vars
predictors = colnames(fred_dt)[2:ncol(fred_dt)]
fred_dt[, (predictors) := lapply(.SD, nafill, type = "locf"), .SDcols = predictors]

# We want to keep only columns that have dates for all observations after some date
dates_start = c(as.Date("1961-06-01"),
                as.Date("1980-01-01"),
                as.Date("1990-01-01"),
                as.Date("2000-01-01"))
columns_save = list()
for (i in seq_along(dates_start)) {
  # filter dates
  d = dates_start[i]
  fred_dt_sample = fred_dt[month >= d]

  # check for missing values
  na_check = colSums(is.na(fred_dt_sample))
  fred_dt_sample = fred_dt_sample[, .SD, .SDcols = na_check == 0]
  columns_save[[i]] = fred_dt_sample[, .(start_date = d,
                                         cols = colnames(fred_dt_sample)[-1])]
}
columns_save = rbindlist(columns_save)

# final dt object with sampled data
vars_keep = c("month", columns_save[, unique(cols)])
fred_sample_by_date = fred_dt[, ..vars_keep]

# save final objects
fwrite(columns_save, "data/fred_col.csv")
fwrite(fred_sample_by_date, "data/fred_sample_by_date.csv")

