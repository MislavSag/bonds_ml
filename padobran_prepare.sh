#!/bin/bash

#PBS -N Bonds
#PBS -l mem=64GB


cd ${PBS_O_WORKDIR}
apptainer run image.sif padobran_prepare.R 1
