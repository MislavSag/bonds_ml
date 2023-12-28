library(data.table)
library(termstrc)
library(lubridate)
library(forecast)
library(gausscov)
library(duckdb)
library(findata)
library(httr)
library(lubridate)
library(runner)
library(mlr3verse)
library(ggplot2)
library(patchwork)
library(paradox)
library(future)



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

# Import Lui and Wu data
lw = fread("data/lw_monthly.csv")
lw = melt(lw, id.vars = "date",
          variable.name = "maturity",
          value.name = "yield",
          variable.factor = FALSE)


# MERGE LUI, WU AND TREASURY DATA ---------------------------------------
# inspect difference
x = lw[maturity == "m120", .(maturity, date, yield)]
x = na.omit(x)
y = yields[maturity == "m120"]
y = y[, .(date, yield_treasury)]

# merge x and y
z = merge(x, y, by = "date")
z = z[, .(date, yield, yield_treasury)]
z = as.xts.data.table(z)
plot(z)
cor(z)
cor(z, method = "spearman")
cor(z, method = "kendall")
difference_series = z[, 1] - z[, 2]
plot(difference_series)
difference_series = as.vector(coredata(difference_series))
t.test(difference_series)

# Merge by backward Ratio - example
new_series = y[date > x[, max(date)], yield_treasury]
old_series = x[date < x[, max(date)], yield]
dates_ = c(x[date < x[, max(date)], date],
           y[date > x[, max(date)], date])
ratio <- new_series[1] / old_series[length(old_series)]
adjusted_old_series <- old_series * ratio
merged_series <- c(adjusted_old_series, new_series)
merged_series = as.data.table(cbind.data.frame(dates_, merged_series))
plot(as.xts.data.table(merged_series))

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

# plot all yields
yieldsadj_plot = dcast(yieldsadj, date ~ maturity)
plot(as.xts.data.table(yieldsadj_plot),
     main = "Yields for different maturities",
     legend.loc = "topright")


# PRICES, ECESS RETURNS AND FORWARDS --------------------------------------
# calculate bond prices
# https://www.math.kth.se/matstat/gru/sf2701/2014/lecture11.pdf
# https://www.uio.no/studier/emner/matnat/math/STK-MAT3700/h22/lecture-notes/lecture4-risk-free-financial-assetes.pdf
yieldsadj[, maturity_months := as.integer(sub("m", "", maturity))]
yieldsadj[, maturity_years := maturity_months / 12]
yieldsadj[, price := exp(-maturity_years * (yield / 100))]
yieldsadj[, price_log := log(price)]

# plot some prices
plot(as.xts.data.table(yieldsadj[maturity == "m12", .(date, price)]))
plot(as.xts.data.table(yieldsadj[maturity == "m60", .(date, price)]))
plot(as.xts.data.table(yieldsadj[maturity == "m120", .(date, price)]))

# calculate excess returns
# excess_returns = function(m = 1) {
#   setorder(yieldsadj, date, maturity_months)
#   yieldsadj[, maturity_shifted := shift(price_log, m), by = date]
#   setorder(yieldsadj, maturity_months, date)
#   yieldsadj[, maturity_shifted := shift(maturity_shifted, -m, type = "shift")]
#   yieldsadj[, annualized_yield := .SD[maturity_months == m, (m/12) * (yield / 100)], by = date]
#   yieldsadj[, annualized_yield := shift(annualized_yield, -1, type = "shift")]
#   setorder(yieldsadj, date, maturity_months)
#   yieldsadj[, excess_return := maturity_shifted - price_log - annualized_yield]
#   setnames(yieldsadj, "excess_return", paste0("excess_return", "_", m))
#   yieldsadj[, `:=`(annualized_yield = NULL, maturity_shifted = NULL)]
#   yieldsadj
# }
excess_returns = function(m = 1) {
  setorder(yieldsadj, maturity_months, date)
  # yieldsadj[, annualized_yield := .SD[maturity_months == m, (m/12) * (yield / 100)], by = date]
  # yieldsadj[, annualized_yield := shift(annualized_yield, -1, type = "shift"), by = maturity]
  # yieldsadj[, excess_return := shift(price_log, -m, type = "shift") - price_log - annualized_yield, by = maturity]
  yieldsadj[, excess_return := shift(price_log, -m, type = "shift") - price_log, by = maturity]
  setnames(yieldsadj, "excess_return", paste0("excess_return", "_", m))
  # yieldsadj[, `:=`(annualized_yield = NULL)]
  yieldsadj
}
excess_returns(1)
excess_returns(3)
excess_returns(6)
excess_returns(12)

# summarize excess returns
# compare with table 1. (page 37) in paper:
# https://deliverypdf.ssrn.com/delivery.php?ID=476013072022123118102073106091102095028032023043029030023010002088116103065127017070033000021120038125121098096017089007086112010060038093078015094071084029081108104056003031101099003110066071088105068009076081087007000104016094125024124110110094088085&EXT=pdf&INDEX=TRUE
# dates_ = c(as.Date("1984-01-01"), as.Date("2018-06-01"))
# # dates_ = c(as.Date("1962-01-01"), as.Date("2019-01-01"))
# dt_ = lw[date %between% dates_]
# dt_[maturity == "m24", PerformanceAnalytics::Return.annualized(excess_return, scale = 12)] # 2
# dt_[maturity == "m36", PerformanceAnalytics::Return.annualized(excess_return, scale = 12)] # 2
# dt_[maturity == "m24", mean(excess_return, na.rm = TRUE) * 100 * 12] # 3
# dt_[maturity == "m36", mean(excess_return, na.rm = TRUE) * 100 * 12] # 3
# dt_[maturity == "m48", mean(excess_return, na.rm = TRUE) * 100 * 12] # 3
# dt_[maturity == "m60", mean(excess_return, na.rm = TRUE) * 100 * 12] # 5
# dt_[maturity == "m72", mean(excess_return, na.rm = TRUE) * 100 * 12] # 6
# dt_[maturity == "m84", mean(excess_return, na.rm = TRUE) * 100 * 12] # 7
# dt_[maturity == "m96", mean(excess_return, na.rm = TRUE) * 100 * 12] # 8
# dt_[maturity == "m108", mean(excess_return, na.rm = TRUE) * 100 * 12] # 9
# dt_[maturity == "m120", mean(excess_return, na.rm = TRUE) * 100 * 12] # 10
# #  1.68 2.44 3.01 3.49 3.99 4.33 4.78 5.03 5.55

# # calculate forward returns
# setorder(yieldsadj, date, maturity_months)
# yieldsadj[, forward_ret_1 := shift(price_log, 1) - price_log, by = date]
# yieldsadj[, forward_ret_3 := shift(price_log, 3) - price_log, by = date]
# yieldsadj[, forward_ret_6 := shift(price_log, 6) - price_log, by = date]
# yieldsadj[, forward_ret_12 := shift(price_log, 12) - price_log, by = date]
#
# # calculte forward spreads
# forward_spreads = function(m = 1) {
#   # yieldsadj[, annualized_yield := .SD[maturity_months == m, (m/12) * (yield / 100)], by = date]
#   # yieldsadj[, annualized_yield := shift(annualized_yield, -1, type = "shift")]
#   cols_ = paste0("forward_ret_", m)
#   yieldsadj[, forward_spread := get(cols_) - annualized_yield]
#   setnames(yieldsadj, "forward_spread", paste0("forward_spread", "_", m))
#   # yieldsadj[, `:=`(annualized_yield = NULL)]
#   yieldsadj
# }
# forward_spreads(1)
# forward_spreads(3)
# forward_spreads(6)
# forward_spreads(12)


# TLT DATA ----------------------------------------------------------------
# get securities data from QC
get_sec = function(symbol) {
  con <- dbConnect(duckdb::duckdb())
  query <- sprintf("
    SELECT *
    FROM 'F:/lean_root/data/all_stocks_daily.csv'
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


# MACRO DATA --------------------------------------------------------------
# import fred series
fred_dt = fread("F:/macro/fred.csv")

# remove unnecessary columns
fred_dt = fred_dt[, .(series_id, date = date_real, frequency_short, popularity,
                      units, id_category, value)]

# filter daily, (be)weekly and monthly data
# fred_dt = fred_dt[frequency_short %in% c("D", "W", "BW", "M")]

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

# define all predictors
predictors = colnames(dt)[(grep("excess_return_12", colnames(dt))+1):ncol(dt)]


# TASKS --------------------------------------------------------
# task parameters
cols = colnames(dt)
task_params = expand.grid(
  gsub("excess_return_", "", cols[grep("excess", cols)]),
  dt[, unique(maturity)],
  stringsAsFactors = FALSE
)
colnames(task_params) = c("horizont", "maturity")

# help function to prepare data for specific maturity and horizont
id_cols = c("date", "maturity")
tasks = lapply(7:12, function(i) {
  # i = 1
  horizont_ = task_params[i, "horizont"]
  mat_ = task_params[i, "maturity"]
  target_ = paste0("excess_return_", horizont_)
  cols_ = c(id_cols, target_, predictors)
  dt_ = dt[, ..cols_]
  dt_ = dt_[maturity == mat_]
  dt_ = na.omit(dt_)
  dt_[, date := as.POSIXct(date, tz = "UTC")]
  tsk_ = as_task_regr(dt_,
                      id = paste(mat_, horizont_, sep = "_"),
                      target = target_)
  tsk_$col_roles$feature = setdiff(tsk_$col_roles$feature,
                                   id_cols)
  tsk_
})


# CROSS VALIDATION --------------------------------------------------------
# create expanding window function
nested_cv_expanding = function(task,
                               train_length_start = 60,
                               tune_length = 3,
                               test_length = 1,
                               gap_tune = 1,
                               gap_test = 1) {

  # get year month id data
  # task = tasks[[2]]$clone()
  task_ = task$clone()
  date_ = task_$backend$data(cols = c("date", "..row_id"),
                             rows = 1:task_$nrow)
  stopifnot(all(task_$row_ids == date_$`..row_id`))
  groups_v = date_[, unlist(unique(date))]

  # create cusom CV's for inner and outer sampling
  custom_inner = rsmp("custom", id = task_$id)
  custom_outer = rsmp("custom", id = task_$id)

  # util vars
  get_row_ids = function(mid) unlist(date_[date %in% mid, 2], use.names = FALSE)
  n = task_$nrow
  mondf = function(d1, d2) { monnb(d2) - monnb(d1) }
  monnb = function(d) {
    lt <- as.POSIXlt(as.Date(d, origin="1900-01-01"))
    lt$year*12 + lt$mon }

  # create train data
  train_groups = lapply(train_length_start:n, function(i) groups_v[1:i])

  # create tune set
  tune_groups <- lapply((train_length_start+gap_tune+1):n, function(i) groups_v[i:(i+tune_length-1)])
  index_keep = vapply(tune_groups, function(x) !any(is.na(x)), FUN.VALUE = logical(1L))
  tune_groups = tune_groups[index_keep]

  # equalize train and tune sets
  train_groups = train_groups[1:length(tune_groups)]

  # create test sets
  insample_length = vapply(train_groups, function(x) as.integer(length(x) + gap_tune + tune_length + gap_test),
                           FUN.VALUE = integer(1))
  test_groups <- lapply(insample_length+1, function(i) groups_v[i:(i+test_length-1)])
  index_keep = vapply(test_groups, function(x) !any(is.na(x)), FUN.VALUE = logical(1L))
  test_groups = test_groups[index_keep]

  # equalize train, tune and test sets
  train_groups = train_groups[1:length(test_groups)]
  tune_groups = tune_groups[1:length(test_groups)]

  # make sets
  train_sets <- lapply(train_groups, get_row_ids)
  tune_sets <- lapply(tune_groups, get_row_ids)
  test_sets <- lapply(test_groups, get_row_ids)

  # test tune and test
  test_1 = vapply(seq_along(train_groups), function(i) {
    mondf(
      tail(as.Date(train_groups[[i]], origin = "1970-01-01"), 1),
      head(as.Date(tune_groups[[i]], origin = "1970-01-01"), 1)
    )
  }, FUN.VALUE = numeric(1L))
  stopifnot(all(test_1 == 1 + gap_tune))
  test_2 = vapply(seq_along(train_groups), function(i) {
    mondf(
      tail(as.Date(tune_groups[[i]], origin = "1970-01-01"), 1),
      head(as.Date(test_groups[[i]], origin = "1970-01-01"), 1)
    )
  }, FUN.VALUE = numeric(1L))
  stopifnot(all(test_2 == 1 + gap_test))
  test_3 = vapply(seq_along(train_groups), function(i) {
    unlist(head(test_sets[[i]], 1) - tail(tune_sets[[i]], 1))
  }, FUN.VALUE = numeric(1L))
  stopifnot(all(test_3 == 1 + gap_test))

  # create inner and outer resamplings
  custom_inner$instantiate(task_, train_sets, tune_sets)
  inner_sets = lapply(seq_along(train_groups), function(i) {
    c(train_sets[[i]], tune_sets[[i]])
  })
  custom_outer$instantiate(task_, inner_sets, test_sets)
  return(list(custom_inner = custom_inner, custom_outer = custom_outer))
}

# create list of cvs
cvs = lapply(tasks, function(tsk_) {
  # tsk_ = tasks[[1]]
  horizont_ = as.integer(gsub("excess_return_", "", tsk_$target_names))
  nested_cv_expanding(
    task = tsk_,
    train_length_start = 120,
    tune_length = 3,
    test_length = 1,
    gap_tune = horizont_ - 1, # TODO: think if here is -1.
    gap_test = horizont_ - 1  # TODO: think if here is -1.
  )
})

# # visualize cvs
# prepare_cv_plot = function(x, set = "train") {
#   x = lapply(x, function(x) data.table(ID = x))
#   x = rbindlist(x, idcol = "fold")
#   x[, fold := as.factor(fold)]
#   x[, set := as.factor(set)]
#   x[, ID := as.numeric(ID)]
# }
# plot_cv = function(i, n = 10) {
#   # i = 2
#   print(i)
#   cv_ = cvs[[i]]
#   task_ = tasks[[i]]
#   cv_test_inner = cv_$custom_inner
#   cv_test_outer = cv_$custom_outer
#
#   # prepare train, tune and test folds
#   train_sets = cv_test_inner$instance$train[1:n]
#   train_sets = prepare_cv_plot(train_sets)
#   tune_sets = cv_test_inner$instance$test[1:n]
#   tune_sets = prepare_cv_plot(tune_sets, set = "tune")
#   test_sets = cv_test_outer$instance$test[1:n]
#   test_sets = prepare_cv_plot(test_sets, set = "test")
#   dt_vis = rbind(train_sets, tune_sets, test_sets)
#   ggplot(dt_vis, aes(x = fold, y = ID, color = set)) +
#     geom_point() +
#     theme_minimal() +
#     coord_flip() +
#     labs(x = "", y = '', title = cv_test_inner$id)
# }
# plots = lapply(c(1, 2, 3), plot_cv, n = 15)
# wp = wrap_plots(plots)
# ggsave("plot_cv.png", plot = wp, width = 10, height = 8, dpi = 300)



# ADD PIPELINES -----------------------------------------------------------
# source pipes, filters and other
# source("mlr3_winsorization.R")
source("mlr3_uniformization.R")
source("mlr3_gausscov_f1st.R")
source("mlr3_gausscov_f3st.R")
# source("mlr3_dropna.R")
# source("mlr3_dropnacol.R")
source("mlr3_filter_drop_corr.R")
# source("mlr3_winsorizationsimple.R")
# source("mlr3_winsorizationsimplegroup.R")
# source("PipeOpPCAExplained.R")
# measures
source("Linex.R")
source("AdjLoss2.R")
source("PortfolioRet.R")

# add my pipes to mlr dictionary
mlr_pipeops$add("uniformization", PipeOpUniform)
# mlr_pipeops$add("winsorize", PipeOpWinsorize)
# mlr_pipeops$add("winsorizesimple", PipeOpWinsorizeSimple)
# mlr_pipeops$add("winsorizesimplegroup", PipeOpWinsorizeSimpleGroup)
# mlr_pipeops$add("dropna", PipeOpDropNA)
# mlr_pipeops$add("dropnacol", PipeOpDropNACol)
mlr_pipeops$add("dropcorr", PipeOpDropCorr)
# mlr_pipeops$add("pca_explained", PipeOpPCAExplained)
mlr_filters$add("gausscov_f1st", FilterGausscovF1st)
mlr_filters$add("gausscov_f3st", FilterGausscovF3st)
mlr_measures$add("linex", Linex)
mlr_measures$add("adjloss2", AdjLoss2)
mlr_measures$add("portfolio_ret", PortfolioRet)


# LEARNERS ----------------------------------------------------------------
# graph template
gr = gunion(list(
  po("nop", id = "nop_union_pca"),
  po("pca", center = FALSE, rank. = 10),
  po("ica", n.comp = 10)
)) %>>% po("featureunion")
graph_template =
  # po("subsample") %>>% # uncomment this for hyperparameter tuning
  # po("dropnacol", id = "dropnacol", cutoff = 0.05) %>>%
  # po("dropna", id = "dropna") %>>%
  po("removeconstants", id = "removeconstants_1", ratio = 0)  %>>%
  po("fixfactors", id = "fixfactors") %>>%
  # po("winsorizesimple", id = "winsorizesimple", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  # po("winsorizesimplegroup", group_var = "weekid", id = "winsorizesimplegroup", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  # po("removeconstants", id = "removeconstants_2", ratio = 0)  %>>%
  po("dropcorr", id = "dropcorr", cutoff = 0.99) %>>%
  # scale branch
  po("branch", options = c("uniformization", "scale"), id = "scale_branch") %>>%
  gunion(list(po("uniformization"),
              po("scale")
  )) %>>%
  po("unbranch", id = "scale_unbranch") %>>%
  # po("dropna", id = "dropna_v2") %>>%
  # add pca columns
  gr %>>%
  # filters
  po("branch", options = c("jmi", "relief", "gausscovf3"), id = "filter_branch") %>>%
  gunion(list(po("filter", filter = flt("jmi"), filter.nfeat = 10),
              po("filter", filter = flt("relief"), filter.nfeat = 10),
              po("filter", filter = flt("gausscov_f3st"), m = 1, p0 = 0.01, filter.cutoff = 0)
              # po("filter", filter = flt("gausscov_f1st"), filter.cutoff = 0)
  )) %>>%
  po("unbranch", id = "filter_unbranch") %>>%
  # modelmatrix
  po("branch", options = c("nop_interaction", "modelmatrix"), id = "interaction_branch") %>>%
  gunion(list(
    po("nop", id = "nop_interaction"),
    po("modelmatrix", formula = ~ . ^ 2))) %>>%
  po("unbranch", id = "interaction_unbranch") %>>%
  po("removeconstants", id = "removeconstants_3", ratio = 0)

# hyperparameters template
graph_template$param_set
search_space_template = ps(
  # subsample for hyperband
  # subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization
  # preprocessing
  # dropnacol.affect_columns = p_fct(
  #   levels = c("0.01", "0.05", "0.10"),
  #   trafo = function(x, param_set) {
  #     switch(x,
  #            "0.01" = 0.01,
  #            "0.05" = 0.05,
  #            "0.10" = 0.1)
  #   }
  # ),
  dropcorr.cutoff = p_fct(
    levels = c("0.80", "0.90", "0.95", "0.99"),
    trafo = function(x, param_set) {
      switch(x,
             "0.80" = 0.80,
             "0.90" = 0.90,
             "0.95" = 0.95,
             "0.99" = 0.99)
    }
  ),
  # dropcorr.cutoff = p_fct(levels = c(0.8, 0.9, 0.95, 0.99)),
  # winsorizesimplegroup.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  # winsorizesimplegroup.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
  # winsorizesimple.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  # winsorizesimple.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
  # scaling
  scale_branch.selection = p_fct(levels = c("uniformization", "scale")),
  # filters
  filter_branch.selection = p_fct(levels = c("jmi", "relief", "gausscovf3")),
  # interaction
  interaction_branch.selection = p_fct(levels = c("nop_interaction", "modelmatrix"))
)

# random forest graph
graph_rf = graph_template %>>%
  po("learner", learner = lrn("regr.ranger"))
plot(graph_rf)
graph_rf = as_learner(graph_rf)
as.data.table(graph_rf$param_set)[, .(id, class, lower, upper, levels)]
search_space_rf = search_space_template$clone()
search_space_rf$add(
  ps(regr.ranger.max.depth  = p_int(1, 15),
     regr.ranger.replace    = p_lgl(),
     regr.ranger.mtry.ratio = p_dbl(0.1, 1),
     regr.ranger.num.trees  = p_int(10, 2000),
     regr.ranger.splitrule  = p_fct(levels = c("variance", "extratrees")))
)
# regr.ranger.min.node.size   = p_int(1, 20), # Adjust the range as needed
# regr.ranger.sample.fraction = p_dbl(0.1, 1),

# xgboost graph
graph_xgboost = graph_template %>>%
  po("learner", learner = lrn("regr.xgboost"))
plot(graph_xgboost)
graph_xgboost = as_learner(graph_xgboost)
as.data.table(graph_xgboost$param_set)[grep("depth", id), .(id, class, lower, upper, levels)]
search_space_xgboost = ps(
  # subsample for hyperband
  # subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization
  # preprocessing
  # dropnacol.affect_columns = p_fct(
  #   levels = c("0.01", "0.05", "0.10"),
  #   trafo = function(x, param_set) {
  #     switch(x,
  #            "0.01" = 0.01,
  #            "0.05" = 0.05,
  #            "0.10" = 0.1)
  #   }
  # ),
  dropcorr.cutoff = p_fct(
    levels = c("0.80", "0.90", "0.95", "0.99"),
    trafo = function(x, param_set) {
      switch(x,
             "0.80" = 0.80,
             "0.90" = 0.90,
             "0.95" = 0.95,
             "0.99" = 0.99)
    }
  ),
  # dropcorr.cutoff = p_fct(levels = c(0.8, 0.9, 0.95, 0.99)),
  # winsorizesimple.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  # winsorizesimple.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
  # scaling
  scale_branch.selection = p_fct(levels = c("uniformization", "scale")),
  # filters
  filter_branch.selection = p_fct(levels = c("jmi", "relief", "gausscovf3")),
  # interaction
  interaction_branch.selection = p_fct(levels = c("nop_interaction", "modelmatrix")),
  # learner
  regr.xgboost.alpha     = p_dbl(0.001, 100, logscale = TRUE),
  regr.xgboost.max_depth = p_int(1, 20),
  regr.xgboost.eta       = p_dbl(0.0001, 1, logscale = TRUE),
  regr.xgboost.nrounds   = p_int(1, 5000),
  regr.xgboost.subsample = p_dbl(0.1, 1)
)


# BENCHMARK ---------------------------------------------------------------
mlr3_save_path = file.path("data/results")
plan("multisession", workers = 4L)
lapply(3:length(cvs), function(i) {
  # debug
  # i = 1
  print(i)

  # get cv and task
  cv_ = cvs[[i]]
  task_ = tasks[[i]]

  # get cv inner object
  cv_inner = cv_$custom_inner
  cv_outer = cv_$custom_outer
  cat("Number of iterations fo cv inner is ", cv_inner$iters, "\n")

  designs_cv_l = lapply(1:cv_inner$iters, function(j) {
    # debug
    # j = 1
    print(cv_inner$id)

    # with new mlr3 version I have to clone
    task_inner = task_$clone()
    task_inner$filter(c(cv_inner$train_set(j), cv_inner$test_set(j)))

    # inner resampling
    custom_ = rsmp("custom")
    custom_$id = paste0("custom_", cv_inner$iters, "_", j)
    custom_$instantiate(task_inner,
                        list(cv_inner$train_set(j)),
                        list(cv_inner$test_set(j)))

    # objects for all autotuners
    measure_ = msr("regr.mse")
    tuner_ = tnr("random_search")
    # tuner_   = tnr("hyperband", eta = 5)
    # tuner_   = tnr("mbo")
    term_evals = 10

    # auto tuner rf
    at_rf = auto_tuner(
      tuner = tuner_,
      learner = graph_rf,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_rf,
      # terminator = trm("none")
      term_evals = term_evals
    )

    # auto tuner xgboost
    at_xgboost = auto_tuner(
      tuner = tuner_,
      learner = graph_xgboost,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_xgboost,
      # terminator = trm("none")
      term_evals = term_evals
    )

    # outer resampling
    customo_ = rsmp("custom")
    customo_$id = paste0("custom_", cv_inner$iters, "_", j)
    customo_$instantiate(task_, list(cv_outer$train_set(j)), list(cv_outer$test_set(j)))

    # nested CV for one round
    design = benchmark_grid(
      tasks = task_,
      learners = list(at_rf, at_xgboost),
      resamplings = customo_
    )
  })
  designs_cv = do.call(rbind, designs_cv_l)

  # benchmark
  system.time({bmr = benchmark(designs_cv, store_models = FALSE)})
  # 100 -> 10230.53 (170 min)

  # save locally and to list
  time_ = format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  file_name = paste0("bonds-", i, "-", time_, ".rds")
  saveRDS(bmr, file.path(mlr3_save_path, file_name))
})













prepare = function(horizont = 3, mat = "m120") {
  er = paste0("excess_return_", horizont)
  cols_ = c("date", "maturity", "excess_return_1", er, predictors)
  dt_ = dt[, ..cols_]
  dt_ = dt_[maturity == mat]
  dt_ = na.omit(dt_)
  non_pred_cols = c("date", "maturity", "excess_return_1", er)
  predictors = setdiff(colnames(dt_), non_pred_cols)
  X = as.matrix(dt_[, ..predictors])
  y = as.matrix(dt_[, get(er)])
  return(list(X = X, y = y, meta = dt_[, .SD, .SDcols = cols_[1:4]]))
}


# FEATURE IMPORTANCE ------------------------------------------------------
# define objects for gausscov
lag_ = 5
dt[, unique(maturity)]
data_ = prepare(horizont = 1, mat = "m120")
X = data_$X
y = data_$y
cols = colnames(X)
cols = rep(cols, each = lag_)
cols[seq(2, length(cols), by = lag_)] <- paste0(cols[seq(2, length(cols), by = lag_)], "_1")
cols[seq(3, length(cols), by = lag_)] <- paste0(cols[seq(3, length(cols), by = lag_)], "_2")
cols[seq(4, length(cols), by = lag_)] <- paste0(cols[seq(4, length(cols), by = lag_)], "_3")
cols[seq(5, length(cols), by = lag_)] <- paste0(cols[seq(5, length(cols), by = lag_)], "_4")
cols[seq(6, length(cols), by = lag_)] <- paste0(cols[seq(6, length(cols), by = lag_)], "_5")
data__ = flag(cbind(y, X), n = nrow(X), i = 1, lag = lag_)
y = data__[[1]]
X = data__[[2]]
X = X[, (lag_+1):ncol(X)]
dim(X)
length(cols)
colnames(X) = cols
# y = as.matrix(X_[[1]])
# X_ = X_[[2]]
# dim(X)
# dim(y)

# insample feature importance using gausscov f1st_res
f1st_res = f1st(y = y, x = X, p0=0.01)
f1st_res = f1st_res[[1]]
f1st_res_index = f1st_res[f1st_res[, 1] != 0, , drop = FALSE]
colnames(X)[f1st_res_index[, 1]]
lm_f1_res = lm(y ~ as.matrix(X[, colnames(X)[f1st_res_index[, 1]]]))
summary(lm_f1_res)

# insample feature importance using gausscov f3st_res
f3st_res = f3st(y = y, x = X, m = 2, p = 0.3)
res_index <- unique(as.integer(f3st_res[[1]][1, ]))[-1]
res_index  <- res_index [res_index  != 0]
colnames(X)[res_index]
# formula_ = paste0("y ~ (", paste0(colnames(X)[res_index], collapse = " + "), ")^2")
# formula_ = as.formula(formula_)
# lm_f3_res = lm(formula_, data = cbind.data.frame(y, X))
lm_f3_res = lm(y ~ X[, colnames(X)[res_index]])
summary(lm_f3_res)
dim(y)
dim(X[, colnames(X)[res_index]])

# tlt prediction
predictions = predict(lm_f3_res)
predictions = as.data.table(cbind.data.frame(month = data_$meta$date[(lag_+1):length(data_$meta$date)], predictions))
predictions[, signal := predictions > 0]
tlt_back = merge(tlt_m, predictions, by = "month", all.x = TRUE, all.y = FALSE)
tlt_back = tlt_back[, strategy := returns * shift(signal)]
tlt_back_xts = na.omit(as.xts.data.table(tlt_back[, .(month, returns, strategy)]))
PerformanceAnalytics::charts.PerformanceSummary(tlt_back_xts)

# predictions using raw data
back = merge(data_$meta, predictions, by.x = "date", by.y = "month", all.x = TRUE, all.y = FALSE)
back = back[, strategy := excess_return_1 * shift(signal)]
back_xts = na.omit(as.xts.data.table(back[, .(date, excess_return_1, strategy)]))
PerformanceAnalytics::charts.PerformanceSummary(back_xts)


# ROLLING GAUSSCOV --------------------------------------------------------
# prepare data
prepare = function(horizont = 3, mat = "m120") {
  er = paste0("excess_return_", horizont)
  cols_ = c("date", "maturity", "excess_return_1", er, predictors_fred)
  dt_ = dt[, ..cols_]
  dt_ = dt_[maturity == mat]
  dt_ = na.omit(dt_)
  return(dt_)
}
data_ = prepare(horizont = 12, mat = "m120")

# WFO
# X = as.matrix(dt_[, ..predictors])
# y = as.matrix(dt_[, get(er)])
# return(list(X = X, y = y, meta = dt_[, .SD, .SDcols = cols_[1:4]]))
at_ = 100:nrow(data_) # nrow(data_)
gaucov_imp_l = runner(
  x = as.data.frame(data_),
  f = function(x) {
    # x = as.data.frame(data_)[1:581, ]
    y = as.matrix(x[, 4])
    X = as.matrix(x[, 5:ncol(x)])
    p_ = 0.01
    res_index = NULL
    while(is.null(res_index)) {
      print(p_)
      f3st_res = f3st(y = y, x = X, m = 2, p = p_)
      res_index = tryCatch({unique(as.integer(f3st_res[[1]][1, ]))[-1]},
                           error = function(e) NULL)
      p_ = p_ + 0.01
    }
    if (is.null(res_index)) {
      return(NULL)
    }
    const_ = 0 %in% res_index
    res_index = res_index[res_index  != 0]
    return(cbind.data.frame(
      x[, 1:3],
      y,
      X[, colnames(X)[res_index], drop = FALSE],
      const = const_))
  },
  at = at_,
  na_pad = TRUE
)

# get column formulas from column names
# lm_l = lapply(1:length(gaucov_imp_l), function(i) {
#   print(i)
#   dt_ = gaucov_imp_l[[i]]
#   if (!is.null(dt_)) {
#     cols_ = paste0(colnames(dt_)[2:ncol(dt_)], collapse = "+")
#     print(cols_)
#     if (cols_ == "X[, colnames(X)[res_index]]") {
#       print(dt_)
#     }
#     formula_ = as.formula(paste0("y~", cols_))
#     res = lm(formula_, data = dt_)
#   }
#   return(res)
# })
lm_l = lapply(gaucov_imp_l, function(dt_) {
  # dt_ = gaucov_imp_l[[1]]
  if (!is.null(dt_)) {
    cols_ = paste0(colnames(dt_)[5:(ncol(dt_)-1)], collapse = "+")
    # if (dt_$const[1] == TRUE) {
    #   formula_ = as.formula(paste0("y~", cols_))
    # } else {
    #   formula_ = as.formula(paste0("y~", cols_))
    # }
    formula_ = as.formula(paste0("y~", cols_))
    res = lm(formula_, data = dt_)
    return(res)
  } else {
    return(NULL)
  }
})
predictions_lm = vapply(lm_l, function(x) {
  # x = lm_l[[1]]
  if (is.null(x)) {
    return(NA_real_)
  } else {
    return(tail(predict(x), 1))
  }
}, FUN.VALUE = numeric(1))

# fill na by locf
predictions_lm_filled = nafill(predictions_lm, type = "locf")

# backtest TLT
predictions = cbind.data.frame(data_[100:nrow(data_), .(month = date)],
                               res = predictions_lm_filled + 0.01)
predictions = as.data.table(predictions)
predictions[, signal := (res) > 0]
tlt_back = merge(tlt_m, predictions, by = "month", all.x = TRUE, all.y = FALSE)
tlt_back = tlt_back[, strategy := returns * shift(signal)]
tlt_back_xts = na.omit(as.xts.data.table(tlt_back[, .(month, returns, strategy)]))
PerformanceAnalytics::charts.PerformanceSummary(tlt_back_xts)


# ROLLING GAUSSCOV IN PARAM SPACE---------------------------------------
# parameters
params = expand.grid(
  dt[, unique(maturity)],
  colnames(dt)[grep("excess", colnames(dt))],
  stringsAsFactors = FALSE)
colnames(params) = c("maturity", "target")

# prepare data
prepare = function(horizont = 3, mat = "m120") {
  er = paste0("excess_return_", horizont)
  cols_ = c("date", "maturity", "excess_return_1", er, predictors_fred)
  dt_ = dt[, ..cols_]
  dt_ = dt_[maturity == mat]
  dt_ = na.omit(dt_)
  return(dt_)
}

# rolling gausscov across parameters
at_ = 60:62 # nrow(dt)
lapply(1:nrow(params), function(i) {
  # i = 1
  maturity_ = params[i, "maturity"]
  target_ = params[i, "target"]
  dt_ = prepare(horizont = as.integer(gsub("excess_return_", "", target_)),
                mat = maturity_)
  gaucov_imp_l = runner(
    x = as.data.frame(dt_),
    f = function(x) {
      # x = as.data.frame(dt_)[1:62, ]
      y = as.matrix(x[, 4])
      X = as.matrix(x[, 5:ncol(x)])
      p_ = 0.01
      res_index = NULL
      while(is.null(res_index)) {
        print(p_)
        f3st_res = f3st(y = y, x = X, m = 2, p = p_)
        res_index = tryCatch({unique(as.integer(f3st_res[[1]][1, ]))[-1]},
                             error = function(e) NULL)
        p_ = p_ + 0.01
      }
      if (is.null(res_index)) {
        return(NULL)
      }
      const_ = 0 %in% res_index
      res_index = res_index[res_index  != 0]
      return(cbind.data.frame(
        x[, 1:3],
        y,
        X[, colnames(X)[res_index], drop = FALSE],
        const = const_))
    },
    at = at_,
    na_pad = TRUE
  )
})


# TASKS -------------------------------------------------------------------
# tasks
prepare = function(horizont = 3, mat = "m120") {
  er = paste0("excess_return_", horizont)
  cols_ = c("date", "maturity", "excess_return_1", er,)
  dt_ = dt[, ..cols_]
  dt_ = dt_[maturity == mat]
  dt_ = na.omit(dt_)
  return(dt_)
}
cols = colnames(dt)
task_params = expand.grid(
  as.integer(gsub("excess_return_", "", cols[grep("excess", cols)])),
  dt[, unique(maturity)],
  stringsAsFactors = FALSE
)
colnames(task_params) = c("horizont", "maturity")
tasks = lapply(1:3, function(i) {
  # i = 1
  params_ = task_params[i, ]
  tsk_ = prepare(horizont = params_$horizont, mat = params_$maturity)
  id_cols = c("date", "maturity", "excess_return_1 ", "weekid")
})




# HELP --------------------------------------------------------------------
# library(readxl)
# # convert raw Liu and Wu data to csv with clean names
# lw = read_excel("data/LW_monthly.xlsx", skip = 7, col_types = "numeric")
# lw = as.data.table(lw)
# setnames(lw, c("date", paste0("m", gsub(" .*", "", colnames(lw)[2:ncol(lw)]))))
# lw[, date := as.Date(paste0(as.character(date), "01"), format = "%Y%m%d")]
# fwrite(lw, "data/lw_monthly.csv")

# # CHATGPT forward rates
# # Calculate log yields for 2-year (24 months) bonds
# n = 24 / 12  # 2 years
# log_yield_2yr = -1/n * log_prices['m24']
#
# # Display the first few rows of the log yields for 2-year bonds
# log_yield_2yr.head()
