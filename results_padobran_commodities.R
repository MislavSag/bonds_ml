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
library(matrixStats)
library(future.apply)


# Creds
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)

# Globals
PATH = "F:/padobran/goldml"

# load registry
reg = loadRegistry(PATH, work.dir=PATH)

# Used memory
reg$status[!is.na(mem.used)]
reg$status[, max(mem.used, na.rm = TRUE)]

# Done jobs
results_files = fs::path_ext_remove(fs::path_file(dir_ls(fs::path(PATH, "results"))))
ids_done = findDone(reg=reg)
ids_done = ids_done[job.id %in% results_files]
ids_notdone = findNotDone(reg=reg)

# # Import results
# chunk2 <- function(x,n) split(x, cut(seq_along(x), n, labels = FALSE))
# id_chunks = chunk2(ids_done[[1]], 4)
# plan("multisession", workers = 4L)
# results = future_lapply(id_chunks, function(x) {
#   reduceResultsBatchmark(ids = x, reg = reg)
# })

# Get results
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

# Import tasks
tasks_files = dir_ls(fs::path(PATH, "problems"))
tasks = lapply(tasks_files, readRDS)
names(tasks) = lapply(tasks, function(t) t$data$id)

# Add backend to predictions
backend_l = lapply(tasks, function(tsk_) {
  # tsk_ = tasks[[1]]
  x = tsk_$data$backend$data(1:tsk_$data$nrow,
                             c("month", "..row_id"))
  setnames(x, c("date", "row_ids"))
  x[, horizont := gsub("target_ret_", "", tsk_$data$target_names)]
  x
})
backends = rbindlist(backend_l, fill = TRUE)
backends[, .N, by = horizont]
backends[, task := "gold"]

# Merge predictions and backends
predictions = backends[, .(date, task, row_ids)][predictions, on = c("task", "row_ids")]

# Measures
source("AdjLoss2.R")
source("PortfolioRet.R")
mlr_measures$add("linex", finautoml::Linex)
mlr_measures$add("adjloss2", AdjLoss2)
mlr_measures$add("portfolio_ret", PortfolioRet)

# merge backs and predictions
predictions[, date := as.Date(date)]



# MARKET DATA] ------------------------------------------------------------
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
dt = get_sec("gld")

# Downsample symbol to monthly
dtm = copy(dt)
dtm[, month := ceiling_date(date, "month")]
dtm = dtm[, tail(.SD, 1), by = month]
dtm[, returns := close / shift(close) - 1]
dtm = na.omit(dtm)

# Add last date if not already in dtm
# add_ = predictions[, data.table::yearmon(max(date))] >
#   dtm[, data.table::yearmon(max(month))]
# if (add_) {
#   tlt_m = rbindlist(list(
#     tlt_m,
#     data.table(month = predictions[, max(date)],
#                date  = predictions[, max(date)],
#                close = 100,
#                returns = 0.01)
#   ))
# }


# BACKTEST ----------------------------------------------------------------
# define backtest data that merge TLT and predictions
backtest_dt = merge(dtm, predictions, by.x = "month", by.y = "date", all.x = TRUE, all.y = FALSE)
backtest_dt = na.omit(backtest_dt, cols = "task")

# benchamrk return (TLT return)
benchmark_performance = dtm[
  ,.(returns = Return.cumulative(returns), ann_returns = Return.annualized(returns, scale = 12))
]

# Backtest
predictions_wide = backtest_dt[, .(month, learner, response)]
predictions_wide = dcast(predictions_wide, month ~ learner, value.var = "response")
predictions_wide[, res_sum := rowSums2(as.matrix(.SD)),
                 .SDcols = c("ranger", "xgboost", "nnet", "glmnet")]
predictions_wide[, signal_ranger := ranger >= 0]
predictions_wide[, signal_xgboost := xgboost >= 0]
predictions_wide[, signal_glmnet := glmnet >= 0]
predictions_wide[, signal_nnet := nnet >= 0]
predictions_wide[, signal_sum := (xgboost + ranger + nnet + glmnet) >= 0]
dt_back = merge(dtm, predictions_wide, by = "month", all.x = TRUE, all.y = FALSE)
dt_back = dt_back[, `:=`(
  strategy_ranger = returns * shift(signal_ranger),
  strategy_xgboost = returns * shift(signal_xgboost),
  strategy_nnet = returns * shift(signal_nnet),
  strategy_glmnet = returns * shift(signal_glmnet),
  strategy_sum = returns * shift(signal_sum)
)]
cols = c("month", "returns", "strategy_ranger", "strategy_xgboost",
         "strategy_nnet", "strategy_glmnet",
         "strategy_sum", "signal_ranger", "signal_xgboost",
         "signal_nnet", "signal_glmnet", "signal_sum")
dt_back = dt_back[, ..cols]
dt_back = na.omit(dt_back)

# Portfolio performance
xts_ = as.xts.data.table(dt_back[, .SD, .SDcols = 1:7])
cumRetx = Return.cumulative(xts_)
annRetx = Return.annualized(xts_, scale=12)
sharpex = SharpeRatio.annualized(xts_, scale=12)
winpctx = as.matrix(as.data.table(lapply(xts_, function(x) length(x[x > 0])/length(x[x != 0]))))
rownames(winpctx) = "Win rate"
annSDx = sd.annualized(xts_, scale=12)
DDs = lapply(xts_, findDrawdowns)
maxDDx = as.matrix(as.data.table(lapply(DDs, function(x) min(x$return))))
rownames(maxDDx) = "Max DD"
maxLx = as.matrix(as.data.table(lapply(DDs, function(x) max(x$length))))
rownames(maxLx) = "Max DD days"
res = rbind(cumRetx, annRetx, sharpex, winpctx, annSDx, maxDDx, maxLx)
res = as.data.table(res, keep.rownames = "var")

# visualize histogram of perfromances by signal_sum and add vertical line for benchmark
melt(res[var == "Cumulative Return"], id.vars = "var") |>
  ggplot(aes(value)) +
  geom_histogram() +
  geom_vline(xintercept = benchmark_performance$returns, color = "red")

# Backtests
charts.PerformanceSummary(xts_)


# ENSAMBLE METHODS --------------------------------------------------------
# ensamble for one month
predictions_ensamble = backtest_dt[, .(month, learner, response)]
predictions_ensamble = predictions_ensamble[, .(response = mean(response, na.rm = TRUE)), by = month]
predictions_ensamble[, signal_strat := response >= 0]
dt_back_ansambl = merge(dtm, predictions_ensamble, by = "month", all.x = TRUE, all.y = FALSE)
dt_back_ansambl = dt_back_ansambl[, `:=`(strategy = returns * shift(signal_strat))]
cols = c("month", "returns", "strategy")
dt_back_ansambl = dt_back_ansambl[, ..cols]
dt_back_ansambl = na.omit(dt_back_ansambl)
charts.PerformanceSummary(as.xts.data.table(dt_back_ansambl))


# QC PREPARE --------------------------------------------------------------
# preapre data or QC and save
qc_data = predictions_wide[, .(month, strategy = signal_ranger)]
qc_data = na.omit(qc_data)
qc_data[, sum(abs(diff(strategy)))] # number of trades
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key="0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ==")
cont = storage_container(BLOBENDPOINT, "qc-backtest")
storage_write_csv(qc_data, cont, "gold_forecasts.csv")
