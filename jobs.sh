#!/bin/bash

#PBS -N BondsML
#PBS -l ncpus=4
#PBS -l mem=8GB
#PBS -J 1-3234
#PBS -o experiments/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R 1
`
# 16170
# 12936
# 16170 - 12936
