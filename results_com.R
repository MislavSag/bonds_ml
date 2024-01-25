library(data.table)
library(mlr3verse)
library(fs)
library(duckdb)
library(lubridate)
library(PerformanceAnalytics)
library(janitor)



# commodity data ----------------------------------------------------------------
# import commodity prices
commodity_prices = fread("data/commodity_prices.csv")
commodity_prices = clean_names(commodity_prices)
commodity_prices[, month := as.Date(paste(gsub("M", "", month), "01"), format = "%Y%m%d")]



# PREDICTIONS -------------------------------------------------------------
# list files
mlr3_save_path = file.path(file.path(getwd(), "data"))
files = list.files(mlr3_save_path, full.names = TRUE, pattern = "commodities-")

# read benchmark results
predictions_l = list()
for (i in 1:length(files)) {
  # debug
  print(i)

  # read banchmark results
  bmr = readRDS(files[i])
  gc()
  bmr_dt = as.data.table(bmr)

  # measures
  source("Linex.R")
  source("AdjLoss2.R")
  source("PortfolioRet.R")
  mlr_measures$add("linex", Linex)
  mlr_measures$add("adjloss2", AdjLoss2)
  mlr_measures$add("portfolio_ret", PortfolioRet)

  # aggregate performances
  # agg_ = bmr$aggregate(msrs(c("regr.mse", "regr.mae", "adjloss2", "linex", "portfolio_ret")))
  # cols = c("task_id", "learner_id", "iters", colnames(agg_)[7:length(colnames(agg_))])
  # agg_ = agg_[, learner_id := gsub(".*regr\\.|\\.tuned", "", learner_id)][, ..cols]

  # get predictions
  task_names = lapply(bmr_dt$task, `[[`, "id")
  learner_names = lapply(bmr_dt$learner, `[[`, "id")
  learner_names = gsub(".*\\.regr\\.|\\.tuned", "", learner_names)
  predictions = lapply(bmr_dt$prediction, function(x) as.data.table(x))
  horizont = lapply(bmr_dt$task, `[[`, "target_names")
  predictions = lapply(seq_along(predictions), function(j)
    cbind(task = task_names[[j]],
          learner = learner_names[[j]],
          predictions[[j]],
          horizont = horizont[[j]]))
  predictions = rbindlist(predictions)

  # merge backs and predictions
  backend = bmr_dt$task[[1]]$backend$data(rows = 1:bmr_dt$task[[1]]$nrow,
                                          cols = bmr_dt$task[[1]]$col_info[, id])
  setnames(backend, "..row_id", "row_ids")
  predictions = backend[predictions, on = c("row_ids")]

  # select cols
  cols = c("row_ids", "var", "horizont", "task", "learner", "truth", "response", "month")
  predictions = predictions[, ..cols]
  # predictions[, month := as.Date(date)]
  predictions_l[[i]] = predictions
}
predictions = rbindlist(predictions_l)
predictions[, horizont := as.integer(gsub(".*_", "", horizont))]
predictions[, unique(task)]


# BACKTEST ----------------------------------------------------------------
# portfoli performance
Performance <- function(dt) {
  # dt = performance[2, returns][[1]]
  xts_ = as.xts.data.table(dt[, .SD, .SDcols = 1:(ncol(dt)-1)])

  cumRetx = Return.cumulative(xts_)
  annRetx = Return.annualized(xts_, scale=12)

  res = rbind(cumRetx, annRetx)
  res = as.data.table(res, keep.rownames = "var")
  # res[, mat := dt[1, mat]]

  # sharpex = SharpeRatio.annualized(x, scale=12)
  # winpctx = length(x[x > 0])/length(x[x != 0])
  # annSDx = sd.annualized(x, scale=12)

  # DDs <- findDrawdowns(x)
  # maxDDx = min(DDs$return)
  # maxLx = max(DDs$length)

  # Perf = c(cumRetx, annRetx, sharpex, winpctx, annSDx, maxDDx, maxLx)
  # names(Perf) = c("Cumulative Return", "Annual Return","Annualized Sharpe Ratio",
  #                 "Win %", "Annualized Volatility", "Maximum Drawdown", "Max Length Drawdown")
  # return(Perf)
}


# backtest function
backtest = function(task, var) {
  # task = predictions[var == "gold"]
  # hor = 1
  predictions_wide = task[, .(month, learner, response)]
  predictions_wide = dcast(predictions_wide, month ~ learner, value.var = "response")
  predictions_wide[, res_sum := ranger + xgboost]
  predictions_wide[, signal_ranger := ranger >= 0]
  predictions_wide[, signal_xgboost := xgboost >= 0]
  predictions_wide[, signal_sum := (xgboost + ranger) >= 0]
  predictions_wide[, month := as.Date(month)]
  cols = c("month", var)
  print(cols)
  commodity_ = commodity_prices[, ..cols]
  back_ = merge(commodity_, predictions_wide, by = "month", all.x = TRUE, all.y = FALSE)
  back_[, returns := get(var) / shift(get(var)) - 1]
  back_ = back_[, `:=`(
    strategy_ranger = returns * shift(signal_ranger),
    strategy_xgboost = returns * shift(signal_xgboost),
    signal_sum = returns * shift(signal_sum)
  )]
  cols = c("month", "returns", "strategy_ranger", "strategy_xgboost",
           "signal_sum")
  back_ = back_[, ..cols]
  back_ = na.omit(back_)
  # back_[, mat := mat]
  # # tlt_back = na.omit(as.xts.data.table(tlt_back))
  # # colnames(tlt_back) = paste0(colnames(tlt_back), "_", task[1, 2])
  return(back_)
}

# backtest grid
performance = predictions[, .(returns = list(backtest(.SD, .BY[[1]]))), by = c("task")]
performance = performance[, do.call(rbind, lapply(returns, Performance))]

# individual backtests
mat_ = predictions[, unique(task)][[5]]
res = backtest(predictions[task == mat_], mat_)
charts.PerformanceSummary(as.xts.data.table(res[, 1:5]))


# BEST TASK ---------------------------------------------------------------
# import benchmark results for best
# bmr = readRDS(files[5])
# gc()
# bmr_dt = as.data.table(bmr)

# check resamplings
bmr_dt$resampling[[1]]$train_set(1)
bmr_dt$resampling[[1]]$test_set(1)

# last predictions
last_predictions = predictions[task == "m12_1"][date == max(date)]
last_predictions[, sum(response)]



# PAPRE TRADE -------------------------------------------------------------


