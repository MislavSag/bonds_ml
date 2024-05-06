#!/bin/bash

#PBS -N GoldsML
#PBS -l ncpus=4
#PBS -l mem=6GB
#PBS -J 1-1000
#PBS -o experiments/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R 0

# 1228
