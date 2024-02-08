#!/bin/bash

#PBS -N Bonds
#PBS -l mem=8GB

cd ${PBS_O_WORKDIR}
apptainer run image.sif padobran_prepare.R
