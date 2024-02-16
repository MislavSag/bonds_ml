
#!/bin/bash

#PBS -N BondsML
#PBS -l ncpus=4
#PBS -l mem=4GB
#PBS -J 1-10000
#PBS -o experiments/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R 0

# 12804
