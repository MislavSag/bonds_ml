#!/bin/bash

#PBS -N BondsML
#PBS -l ncpus=4
#PBS -l mem=4GB
#PBS -J 1-3842
#PBS -o experiments/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R 1
`
# 16170
# 12328
# 16170 - 12328
