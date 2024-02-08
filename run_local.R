library(data.table)
library(mlr3batchmark)
library(batchtools)


# load registry
reg = loadRegistry("experiments_test2", writeable = TRUE)

# test 1 job
result = testJob(1, external = TRUE, reg = reg)

# get nondone jobs
ids = findNotDone(reg = reg)

# set up cluster (for local it is parallel)
cf = makeClusterFunctionsSocket(ncpus = 8L)
reg$cluster.functions = cf
saveRegistry(reg = reg)

# define resources and submit jobs
resources = list(ncpus = 2, memory = 8000)
submitJobs(ids = ids$job.id, resources = resources, reg = reg)
