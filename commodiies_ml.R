library(data.table)
library(gausscov)
library(httr)
library(mlr3verse)
library(paradox)
library(AzureStor)


# PARAMETERS --------------------------------------------------------------
# choose commodities
COMMODITIES = "gold" # if NULL, all commodities are used
LIVE        = FALSE


# IMPORT DATA -------------------------------------------------------------
# get data generated in prepare_commodities
if (interactive()) {
  dt = fread("data/commodities_dt.csv")
} else {
  endpoint = "https://snpmarketdata.blob.core.windows.net/"
  key = readLines('./blob_key.txt')
  BLOBENDPOINT = storage_endpoint(endpoint, key=key)
  cont = storage_container(BLOBENDPOINT, "qc-backtest")
  dt = storage_read_csv(cont, "commodities_dt.csv")
}


# TASKS --------------------------------------------------------
# task parameters
cols = colnames(dt)
vars = ifelse(is.null(COMMODITIES), dt[, unique(var)], COMMODITIES)
task_params = expand.grid(gsub("excess_return_", "", cols[grep("target", cols)]),
                          vars, stringsAsFactors = FALSE)
colnames(task_params) = c("targets", "commodity")

# define predictors
cols = colnames(dt)
predictors = cols[(which(cols == "target_ret_1") + 1):length(cols)]

# help function to prepare data for specific maturity and horizont
id_cols = c("month", "var")
tasks = lapply(1:nrow(task_params), function(i) {
  # i = 1
  target_ = task_params[i, "targets"]
  var_ = task_params[i, "commodity"]
  cols_ = c(id_cols, target_, predictors)
  dt_ = dt[, ..cols_]
  dt_ = dt_[var == var_]
  dt_ = na.omit(dt_)
  dt_ = dt_[80:nrow(dt_)] # mannual inspection; prices doesnt change before
  dt_[, month := as.POSIXct(month, tz = "UTC")]
  tsk_ = as_task_regr(dt_,
                      id = paste(var_, sep = "_"),
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
  date_ = task_$backend$data(cols = c("month", "..row_id"),
                             rows = 1:task_$nrow)
  stopifnot(all(task_$row_ids == date_$`..row_id`))
  groups_v = date_[, unlist(unique(month))]

  # create cusom CV's for inner and outer sampling
  custom_inner = rsmp("custom", id = task_$id)
  custom_outer = rsmp("custom", id = task_$id)

  # util vars
  get_row_ids = function(mid)
    unlist(date_[month %in% mid, 2], use.names = FALSE)
  n = task_$nrow
  mondf = function(d1, d2) {
    monnb(d2) - monnb(d1)
  }
  monnb = function(d) {
    lt <- as.POSIXlt(as.Date(d, origin = "1900-01-01"))
    lt$year * 12 + lt$mon
  }

  # create train data
  train_groups = lapply(train_length_start:n, function(i)
    groups_v[1:i])

  # create tune set
  tune_groups <-
    lapply((train_length_start + gap_tune + 1):n, function(i)
      groups_v[i:(i + tune_length - 1)])
  index_keep = vapply(tune_groups, function(x)
    ! any(is.na(x)), FUN.VALUE = logical(1L))
  tune_groups = tune_groups[index_keep]

  # equalize train and tune sets
  train_groups = train_groups[1:length(tune_groups)]

  # create test sets
  insample_length = vapply(train_groups, function(x)
    as.integer(length(x) + gap_tune + tune_length + gap_test),
    FUN.VALUE = integer(1))
  test_groups <-
    lapply(insample_length + 1, function(i)
      groups_v[i:(i + test_length - 1)])
  index_keep = vapply(test_groups, function(x)
    ! any(is.na(x)), FUN.VALUE = logical(1L))
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
    mondf(tail(as.Date(train_groups[[i]], origin = "1970-01-01"), 1),
          head(as.Date(tune_groups[[i]], origin = "1970-01-01"), 1))
  }, FUN.VALUE = numeric(1L))
  stopifnot(all(test_1 == 1 + gap_tune))
  test_2 = vapply(seq_along(train_groups), function(i) {
    mondf(tail(as.Date(tune_groups[[i]], origin = "1970-01-01"), 1),
          head(as.Date(test_groups[[i]], origin = "1970-01-01"), 1))
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
  horizont_ = as.integer(gsub("target_ret_", "", tsk_$target_names))
  nested_cv_expanding(
    task = tsk_,
    train_length_start = 360,
    tune_length = 6,
    test_length = 1,
    gap_tune = horizont_, # TODO: think if here is -1.
    gap_test = horizont_  # TODO: think if here is -1.
  )
})


# ADD PIPELINES -----------------------------------------------------------
print("Add pipelines")

# measures
source("AdjLoss2.R")
source("PortfolioRet.R")

# add my pipes to mlr dictionary
mlr_pipeops$add("uniformization", finautoml::PipeOpUniform)
# mlr_pipeops$add("winsorize", PipeOpWinsorize)
mlr_pipeops$add("winsorizesimple", finautoml::PipeOpWinsorizeSimple)
# mlr_pipeops$add("winsorizesimplegroup", PipeOpWinsorizeSimpleGroup)
mlr_pipeops$add("dropna", finautoml::PipeOpDropNA)
mlr_pipeops$add("dropnacol", finautoml::PipeOpDropNACol)
mlr_pipeops$add("dropcorr", finautoml::PipeOpDropCorr)
# mlr_pipeops$add("pca_explained", PipeOpPCAExplained)
mlr_pipeops$add("filter_target", finautoml::PipeOpFilterRegrTarget)
mlr_filters$add("gausscov_f1st", finautoml::FilterGausscovF1st)
mlr_measures$add("linex", finautoml::Linex)
mlr_measures$add("adjloss2", AdjLoss2)
mlr_measures$add("portfolio_ret", PortfolioRet)


# LEARNERS ----------------------------------------------------------------
# graph template
gr = gunion(list(
  po("nop", id = "nop_union_pca"),
  po("pca", center = FALSE, rank. = 10),
  po("ica", n.comp = 10)
)) %>>% po("featureunion")
filters_ = list(
  po("filter", flt("disr"), filter.nfeat = 3),
  po("filter", flt("jmim"), filter.nfeat = 3),
  po("filter", flt("jmi"), filter.nfeat = 3),
  po("filter", flt("mim"), filter.nfeat = 3),
  po("filter", flt("mrmr"), filter.nfeat = 3),
  po("filter", flt("njmim"), filter.nfeat = 3),
  po("filter", flt("cmim"), filter.nfeat = 3),
  po("filter", flt("carscore"), filter.nfeat = 3),
  po("filter", flt("information_gain"), filter.nfeat = 3),
  po("filter", filter = flt("relief"), filter.nfeat = 3),
  po("filter", filter = flt("gausscov_f1st"), p0 = 0.1, filter.cutoff = 0)
)
graph_filters = gunion(filters_) %>>%
  po("featureunion", length(filters_), id = "feature_union_filters")
graph_template =
  po("removeconstants", id = "removeconstants_1", ratio = 0)  %>>%
  po("fixfactors", id = "fixfactors") %>>%
  po("dropcorr", id = "dropcorr", cutoff = 0.99) %>>%
  # scale branch
  po("branch", options = c("uniformization", "scale"), id = "scale_branch") %>>%
  gunion(list(po("uniformization"),
              po("scale")
  )) %>>%
  po("unbranch", id = "scale_unbranch") %>>%
  gr %>>%
  graph_filters %>>%
  # modelmatrix
  po("branch", options = c("nop_interaction", "modelmatrix"), id = "interaction_branch") %>>%
  gunion(list(
    po("nop", id = "nop_interaction"),
    po("modelmatrix", formula = ~ . ^ 2))) %>>%
  po("unbranch", id = "interaction_unbranch") %>>%
  po("removeconstants", id = "removeconstants_3", ratio = 0)

# hyperparameters template
as.data.table(graph_template$param_set)[101:120]
search_space_template = ps(
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
  # scaling
  scale_branch.selection = p_fct(levels = c("uniformization", "scale")),
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

# xgboost graph
graph_xgboost = graph_template %>>%
  po("learner", learner = lrn("regr.xgboost"))
plot(graph_xgboost)
graph_xgboost = as_learner(graph_xgboost)
as.data.table(graph_xgboost$param_set)[grep("depth", id), .(id, class, lower, upper, levels)]
search_space_xgboost = ps(
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
  # scaling
  scale_branch.selection = p_fct(levels = c("uniformization", "scale")),
  # learner
  regr.xgboost.alpha     = p_dbl(0.001, 100, logscale = TRUE),
  regr.xgboost.max_depth = p_int(1, 20),
  regr.xgboost.eta       = p_dbl(0.0001, 1, logscale = TRUE),
  regr.xgboost.nrounds   = p_int(1, 5000),
  regr.xgboost.subsample = p_dbl(0.1, 1)
)

# glmnet graph
graph_glmnet = graph_template %>>%
  po("learner", learner = lrn("regr.glmnet"))
graph_glmnet = as_learner(graph_glmnet)
as.data.table(graph_glmnet$param_set)[, .(id, class, lower, upper, levels)]
search_space_glmnet = ps(
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
  # scaling
  scale_branch.selection = p_fct(levels = c("uniformization", "scale")),
  # interaction
  interaction_branch.selection = p_fct(levels = c("nop_interaction", "modelmatrix")),
  # learner
  regr.glmnet.s     = p_int(lower = 5, upper = 30),
  regr.glmnet.alpha = p_dbl(lower = 1e-4, upper = 1, logscale = TRUE)
)

# nnet graph
graph_nnet = graph_template %>>%
  po("learner", learner = lrn("regr.nnet", MaxNWts = 50000))
graph_nnet = as_learner(graph_nnet)
as.data.table(graph_nnet$param_set)[, .(id, class, lower, upper, levels)]
search_space_nnet = search_space_template$clone()
search_space_nnet$add(
  ps(regr.nnet.size  = p_int(lower = 2, upper = 15),
     regr.nnet.decay = p_dbl(lower = 0.0001, upper = 0.1),
     regr.nnet.maxit = p_int(lower = 50, upper = 500))
)

# Threads
threads = 2
set_threads(graph_rf, n = threads)
set_threads(graph_xgboost, n = threads)
set_threads(graph_nnet, n = threads)
set_threads(graph_glmnet, n = threads)


# BATCHMARK ---------------------------------------------------------------
# batchmark
designs_l = lapply(seq_along(cvs), function(i) {
  # for (i in 1:length(cvs)) {
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

  # Only last for Live
  if (LIVE) {
    loop_ind = cv_inner$iters
  } else {
    loop_ind = 1:cv_inner$iters
  }

  designs_cv_l = lapply(loop_ind, function(j) {
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
    term_evals = 50

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

    # auto tuner glmnet
    at_glmnet = auto_tuner(
      tuner = tuner_,
      learner = graph_glmnet,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_glmnet,
      # terminator = trm("none")
      term_evals = term_evals
    )

    # auto tuner nnet
    at_nnet = auto_tuner(
      tuner = tuner_,
      learner = graph_nnet,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_nnet,
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
      learners = list(at_rf, at_xgboost, at_glmnet, at_nnet),
      resamplings = customo_
    )
    # populate registry with problems and algorithms to form the jobs
    # print("Batchmark")
    # batchmark(design, reg = reg)
  })
  designs_cv = do.call(rbind, designs_cv_l)
})
designs = do.call(rbind, designs_l)

# exp dir
if (LIVE) {
  dirname_ = "experiments_live"
  if (dir.exists(dirname_)) system(paste0("rm -r ", dirname_))

} else {
  dirname_ = "experiments"
}

# create registry
print("Create registry")
packages = c("data.table", "gausscov", "paradox", "mlr3", "mlr3pipelines",
             "mlr3tuning", "mlr3misc", "future", "future.apply",
             "mlr3extralearners", "stats")
reg = makeExperimentRegistry(file.dir = dirname_, seed = 1, packages = packages)

# populate registry with problems and algorithms to form the jobs
print("Batchmark")
batchmark(designs, reg = reg)

# create sh file
if (LIVE) {
  # load registry
  # reg = loadRegistry("experiments_live", writeable = TRUE)
  # test 1 job
  # result = testJob(1, external = TRUE, reg = reg)
  # user  system elapsed
  # 0.70    0.72  781.16

  # get nondone jobs
  ids = findNotDone(reg = reg)

  # set up cluster (for local it is parallel)
  cf = makeClusterFunctionsSocket(ncpus = 4L)
  reg$cluster.functions = cf
  saveRegistry(reg = reg)

  # define resources and submit jobs
  resources = list(ncpus = 2, memory = 8000)
  submitJobs(ids = ids$job.id, resources = resources, reg = reg)
} else {
  # save registry
  print("Save registry")
  saveRegistry(reg = reg)

  sh_file = sprintf(
    "
#!/bin/bash

#PBS -N ZSEML
#PBS -l ncpus=4
#PBS -l mem=4GB
#PBS -J 1-%d
#PBS -o %s/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R 0
",
    nrow(designs), dirname_
  )
  sh_file_name = "jobs.sh"
  file.create(sh_file_name)
  writeLines(sh_file, sh_file_name)
}

# Inspect individual result
if (LIVE) {
  # load registry
  reg = loadRegistry("experiments_live", writeable = TRUE)

  # import results
  results_live = reduceResultsBatchmark(reg = reg)
  results_live_dt = as.data.table(results_live)
  head(results_live_dt)

  # Get predictions
  predictions = lapply(results_live_dt$prediction, as.data.table)
  predictions = rbindlist(predictions)

  # Merge tak id to get maturity
  ids_ = vapply(results_live_dt$task,
                function(tsk_) tsk_$id,
                FUN.VALUE = character(1))
  predictions = cbind(predictions, ids_)

  # It is ok for 4 folds to have lower row_ids values.
  # This is because one task in a list of tasks have lower number of observations
  # (this is task with 120 maturity or 10 years maturity). It starts from 1971
  # while other starts from 1961.

  # The question is which model or combination of models to use for final prediction.
  # From results in backtest it seems the two best options are:
  # 1. mean acroos all predictions (all maturities and models)
  # 2. mean of predictions for all models but only for maturity 60

  # Get ansamble predictio (mean) that was best on backtest
  predictions[, mean(response)] # 1)
  predictions[ids_ == "m60_1"][, mean(response)] # 2)
  best_prediction = predictions[, mean(response)]

  # Save best prediction to Azure csv
  cont = storage_container(BLOBENDPOINT, "qc-live")
  time_ = strftime(Sys.time(), format = "%Y%m%d%H%M%S")
  file_name = glue("tlt_macro_prediction_{time_}.csv")
  storage_write_csv(
    object = data.frame(prediction_tlt = best_prediction),
    container = cont,
    file = file_name
  )
}

