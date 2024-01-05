library(data.table)
library(gausscov)
library(httr)
library(mlr3verse)
library(paradox)
# library(AzureStor)
# library(mlr3batchmark)
# library(batchtools)



# IMPORT DATA -------------------------------------------------------------
# get data generated in prepare_commodities
dt = fread("data/commodities_dt.csv")


# TASKS --------------------------------------------------------
# task parameters
cols = colnames(dt)
task_params = expand.grid(gsub("excess_return_", "", cols[grep("target", cols)]),
                          dt[, unique(var)],
                          stringsAsFactors = FALSE)
colnames(task_params) = c("targets", "commodity")

# define predictors
cols = colnames(dt)
predictors = cols[(which(cols == "target_ret_12") + 1):length(cols)]

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
    train_length_start = 120,
    tune_length = 3,
    test_length = 1,
    gap_tune = horizont_ - 1,
    # TODO: think if here is -1.
    gap_test = horizont_ - 1  # TODO: think if here is -1.
  )
})


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
  po("branch",
     options = c("uniformization", "scale"),
     id = "scale_branch") %>>%
  gunion(list(po("uniformization"),
              po("scale"))) %>>%
  po("unbranch", id = "scale_unbranch") %>>%
  # po("dropna", id = "dropna_v2") %>>%
  # add pca columns
  gr %>>%
  # filters
  po("branch",
     options = c("jmi", "relief", "gausscovf3"),
     id = "filter_branch") %>>%
  gunion(list(
    po("filter", filter = flt("jmi"), filter.nfeat = 10),
    po(
      "filter",
      filter = flt("relief"),
      filter.nfeat = 10
    ),
    po(
      "filter",
      filter = flt("gausscov_f3st"),
      m = 1,
      p0 = 0.01,
      filter.cutoff = 0
    )
    # po("filter", filter = flt("gausscov_f1st"), filter.cutoff = 0)
  )) %>>%
  po("unbranch", id = "filter_unbranch") %>>%
  # modelmatrix
  po("branch",
     options = c("nop_interaction", "modelmatrix"),
     id = "interaction_branch") %>>%
  gunion(list(
    po("nop", id = "nop_interaction"),
    po("modelmatrix", formula = ~ . ^ 2)
  )) %>>%
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
      switch(
        x,
        "0.80" = 0.80,
        "0.90" = 0.90,
        "0.95" = 0.95,
        "0.99" = 0.99
      )
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
  ps(
    regr.ranger.max.depth  = p_int(1, 15),
    regr.ranger.replace    = p_lgl(),
    regr.ranger.mtry.ratio = p_dbl(0.1, 1),
    regr.ranger.num.trees  = p_int(10, 2000),
    regr.ranger.splitrule  = p_fct(levels = c("variance", "extratrees"))
  )
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
      switch(
        x,
        "0.80" = 0.80,
        "0.90" = 0.90,
        "0.95" = 0.95,
        "0.99" = 0.99
      )
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
# benchamrk
mlr3_save_path = "data"
for (i in 177:180) {
  # 1:length(cvs)
  # debug
  # i = 177
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
    customo_$instantiate(task_,
                         list(cv_outer$train_set(j)),
                         list(cv_outer$test_set(j)))

    # nested CV for one round
    design = benchmark_grid(
      tasks = task_,
      learners = list(at_rf, at_xgboost),
      resamplings = customo_
    )
  })
  designs_cv = do.call(rbind, designs_cv_l)

  # populate registry with problems and algorithms to form the jobs
  print("Benchmark")
  bmr = benchmark(designs_cv, store_models = FALSE)

  # save locally and to list
  time_ = format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  file_name = paste0("commodities-", i, "-", time_, ".rds")
  saveRDS(bmr, file.path(mlr3_save_path, file_name))
}

# benchmark results
bmr$score(msr("regr.mse"))
