#!/bin/bash 
set -e x
for year in {1980..1989}
do 
  cdo -L -O -mergetime  outputs/chelsa_CA_${year}??.nc outputs/chelsa_CA_${year}.nc 
done 

