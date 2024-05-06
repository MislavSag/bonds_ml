#!/bin/bash

#PBS -N commodities
#PBS -l mem=64GB


cd ${PBS_O_WORKDIR}
apptainer run image.sif commodiies_ml.R
