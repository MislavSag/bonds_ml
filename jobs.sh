#!/bin/bash

#PBS -N ZSEML
#PBS -l ncpus=4
#PBS -l mem=6GB
#PBS -J 10001-13464
#PBS -o experiments/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R 0

# 13464
