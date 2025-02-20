library(fastverse)
library(finutils)
library(lubridate)
library(PerformanceAnalytics)
library(ggplot2)


# Import data
predictors = fread("data/bonds-predictors-month-20250220.csv")

# Check some predictors
ggplot(predictors, aes(Slope_US)) +
  geom_histogram()
ggplot(predictors, aes(Level_US)) +
  geom_histogram()
ggplot(predictors, aes(Curvature_US)) +
  geom_histogram()

# Import TLT data
tlt = qc_daily(file_path = "/home/sn/lean/data/stocks_daily.csv",
               symbols = "tlt")
tlt[, month := ceiling_date(date, unit = "month")]
tltm = tlt[, .(close = data.table::last(close),
               open = data.table::first(open)), by = month]
tltm[, target := close / open - 1]
tltm[, target := shift(target, 1, type = "lead")]

# Merge TLT and predictors
back = unique(predictors[, .(month = date, Curvature_US, Level_US, Slope_US, m12_m3, m120_m12)])[
  tltm[, .SD, .SDcols = -c("close", "open")], on = "month"][order(month)]
back = na.omit(back)

# Backtest strategy that buys TLT when yield curve is increasing
back[, strategy := fifelse(m120_m12 > 0, target, 0)]
backxts = as.xts.data.table(back[, .(month, strategy, target)])
charts.PerformanceSummary(backxts)

