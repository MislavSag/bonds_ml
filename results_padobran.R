library(fs)
library(data.table)
library(mlr3verse)
library(mlr3batchmark)
library(batchtools)
library(duckdb)
library(PerformanceAnalytics)
library(AzureStor)
library(future.apply)
library(lubridate)
library(ggplot2)


# creds
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)

# globals
PATH = "F:/padobran/bondsml_2" # "F:/padobran/bonds_ml/"

# load registry
reg = loadRegistry(PATH, work.dir=PATH)

# used memory
reg$status[!is.na(mem.used)]
reg$status[, max(mem.used, na.rm = TRUE)]

# done jobs
results_files = fs::path_ext_remove(fs::path_file(dir_ls(fs::path(PATH, "results"))))
ids_done = findDone(reg=reg)
ids_done = ids_done[job.id %in% results_files]
ids_notdone = findNotDone(reg=reg)

# get results
tabs = batchtools::getJobTable(ids_done, reg = reg)[
  , c("job.id", "job.name", "repl", "prob.pars", "algo.pars"), with = FALSE]
predictions_meta = cbind.data.frame(
  id = tabs[, job.id],
  task = vapply(tabs$prob.pars, `[[`, character(1L), "task_id"),
  learner = gsub(".*regr.|.tuned", "", vapply(tabs$algo.pars, `[[`, character(1L), "learner_id")),
  cv = gsub("custom_|_.*", "", vapply(tabs$prob.pars, `[[`, character(1L), "resampling_id")),
  fold = gsub("custom_\\d+_", "", vapply(tabs$prob.pars, `[[`, character(1L), "resampling_id"))
)
predictions_l = lapply(unlist(ids_done), function(id_) {
  # id_ = 10035
  x = tryCatch({readRDS(fs::path(PATH, "results", id_, ext = "rds"))},
               error = function(e) NULL)
  if (is.null(x)) {
    print(id_)
    return(NULL)
  }
  x["id"] = id_
  x
})
predictions = lapply(predictions_l, function(x) {
  cbind.data.frame(
    id = x$id,
    row_ids = x$prediction$test$row_ids,
    truth = x$prediction$test$truth,
    response = x$prediction$test$response
  )
})
predictions = rbindlist(predictions)
predictions = merge(predictions_meta, predictions, by = "id")
predictions = as.data.table(predictions)

# import tasks
tasks_files = dir_ls(fs::path(PATH, "problems"))
tasks = lapply(tasks_files, readRDS)
names(tasks) = lapply(tasks, function(t) t$data$id)
tasks

# add backend to predictions
backend_l = lapply(tasks, function(tsk_) {
  x = tsk_$data$backend$data(1:tsk_$data$nrow,
                             c("date", "maturity", "..row_id"))
  setnames(x, c("date", "maturity", "row_ids"))
  x[, horizont := gsub("excess_return_", "", tsk_$data$target_names)]
  x
})
backends = rbindlist(backend_l, fill = TRUE)
backends[, .N, by = horizont]
backends[, task := paste0(maturity, "_", horizont)]

# merge predictions and backends
predictions = backends[, .(date, task, row_ids)][predictions, on = c("task", "row_ids")]

# measures
source("AdjLoss2.R")
source("PortfolioRet.R")
mlr_measures$add("linex", finautoml::Linex)
mlr_measures$add("adjloss2", AdjLoss2)
mlr_measures$add("portfolio_ret", PortfolioRet)

# merge backs and predictions
predictions[, date := as.Date(date)]


# TLT DATA ----------------------------------------------------------------
# get securities data from QC
get_sec = function(symbol) {
  con <- dbConnect(duckdb::duckdb())
  query <- sprintf("
    SELECT *
    FROM 'F:/lean/data/stocks_daily.csv'
    WHERE Symbol = '%s'
", symbol)
  data_ <- dbGetQuery(con, query)
  dbDisconnect(con)
  data_ = as.data.table(data_)
  data_ = data_[, .(date = Date, close = `Adj Close`)]
  data_[, returns := close / shift(close) - 1]
  data_ = na.omit(data_)
  return(data_)
}
tlt_dt = get_sec("tlt")

# downsample TLT to monthly
tlt_m = copy(tlt_dt)
tlt_m[, month := ceiling_date(date, "month")]
# tlt_m[, close := tail(close, 1), by = month]
tlt_m = tlt_m[, tail(.SD, 1), by = month]
tlt_m[, returns := close / shift(close) - 1]
tlt_m = na.omit(tlt_m)


# BACKTEST ----------------------------------------------------------------
# define backtest data that merge TLT and predictions
backtest_dt = merge(tlt_m, predictions, by.x = "month", by.y = "date", all.x = TRUE, all.y = FALSE)
backtest_dt = na.omit(backtest_dt, cols = "task")

# benchamrk return (TLT return)
benchmark_performance = tlt_m[,
  .(returns = Return.cumulative(returns), ann_returns = Return.annualized(returns, scale = 12))
]

# portfoli performance
Performance <- function(dt) {
  # dt = performance[2, returns][[1]]
  xts_ = as.xts.data.table(dt[, .SD, .SDcols = 1:(ncol(dt)-1)])

  cumRetx = Return.cumulative(xts_)
  annRetx = Return.annualized(xts_, scale=12)

  res = rbind(cumRetx, annRetx)
  res = as.data.table(res, keep.rownames = "var")
  res[, mat := dt[1, mat]]

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
backtest = function(task, mat) {
  print(mat)
  # task = backtest_dt[task == "m24_1"]
  # mat = "m24_1"
  predictions_wide = task[, .(month, learner, response)]
  predictions_wide = dcast(predictions_wide, month ~ learner, value.var = "response")
  predictions_wide[, res_sum := ranger + xgboost]
  predictions_wide[, signal_ranger := ranger >= 0]
  predictions_wide[, signal_xgboost := xgboost >= 0]
  predictions_wide[, signal_sum := (xgboost + ranger) >= 0]
  tlt_back = merge(tlt_m, predictions_wide, by = "month", all.x = TRUE, all.y = FALSE)
  tlt_back = tlt_back[, `:=`(
    strategy_ranger = returns * shift(signal_ranger),
    strategy_xgboost = returns * shift(signal_xgboost),
    strategy_sum = returns * shift(signal_sum)
  )]
  cols = c("month", "returns", "strategy_ranger", "strategy_xgboost",
           "strategy_sum", "signal_ranger", "signal_xgboost", "signal_sum")
  tlt_back = tlt_back[, ..cols]
  tlt_back = na.omit(tlt_back)
  tlt_back[, mat := mat]
  # tlt_back = na.omit(as.xts.data.table(tlt_back))
  # colnames(tlt_back) = paste0(colnames(tlt_back), "_", task[1, 2])
  tlt_back
}

# backtest grid
performance = backtest_dt[, .(returns = list(backtest(.SD, .BY))), by = "task"]
performance_dt = performance[, do.call(rbind, lapply(returns, Performance))]
performance_dt = performance_dt[var == "Cumulative Return"][order(strategy_sum)]

# visualize histogram of perfromances by signal_sum and add vertical line for benchmark
ggplot(performance_dt, aes(strategy_sum)) +
  geom_histogram(bins = 50) +
  geom_vline(xintercept = benchmark_performance$returns, color = "red")
  # facet_wrap(~ mat, ncol = 2)

# performance acroos maturity and horizont
performance_dt[, `:=`(
  maturiy = as.factor(gsub("m|_.*", "", mat)),
  horizont = as.factor(gsub(".*_", "", mat))
)]
performance_dt[, unique(maturiy)]
lvls = as.character(sort(as.integer(levels(performance_dt[, unique(maturiy)]))))
performance_dt[, maturiy := factor(maturiy, lvls)]
performance_dt[, horizont := factor(horizont, levels = c("1"))]
ggplot(performance_dt, aes(maturiy, strategy_sum)) +
  geom_boxplot() +
  facet_wrap(~ horizont, ncol = 4)

# individual backtests
task_best = performance_dt[which.max(strategy_ranger), paste0("m", maturiy, "_", horizont)]
best = performance[task == task_best, returns][[1]]
charts.PerformanceSummary(as.xts.data.table(best[, 1:5]))


# ENSAMBLE METHODS --------------------------------------------------------
# ensamble for one month
predictions_ensamble = backtest_dt[gsub(".*_", "", task) == 1, .(month, learner, response)]
predictions_ensamble = predictions_ensamble[, .(response = median(response)), by = month]
predictions_ensamble[, signal_strat := response >= 0]
tlt_back = merge(tlt_m, predictions_ensamble, by = "month", all.x = TRUE, all.y = FALSE)
tlt_back = tlt_back[, `:=`(strategy = returns * shift(signal_strat))]
cols = c("month", "returns", "strategy")
tlt_back = tlt_back[, ..cols]
tlt_back = na.omit(tlt_back)
charts.PerformanceSummary(as.xts.data.table(tlt_back))


# QC PREPARE --------------------------------------------------------------
# preapre data or QC and save
qc_data = predictions_ensamble[, .(month, strategy = signal_strat)]
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key="0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ==")
cont = storage_container(BLOBENDPOINT, "qc-backtest")
storage_write_csv(qc_data, cont, "tlt_forecasts.csv") # , col_names = FALSE
