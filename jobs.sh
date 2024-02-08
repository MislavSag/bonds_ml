
#!/bin/bash

#PBS -N BondsML
#PBS -l ncpus=4
#PBS -l mem=4GB
#PBS -J 1-44
#PBS -o experiments_test2/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R 0

