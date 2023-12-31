#!/bin/bash

#PBS -N BondsJobs
#PBS -l ncpus=1
#PBS -l mem=2GB
#PBS -J 20001-30000
#PBS -o experiments/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R 0
