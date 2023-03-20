#!/bin/bash 
# select the Central Asian domain from CHELSA
# for feeding to the DeepSD for downsclaing 
# first I test the precipitation and compare it with the 
# CCLM run driven by MIP-ESM-HR 

dir_to_CHELSA="/p/projects/isimip/isimip/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5v1.0/"
#chelsa-w5e5v1.0_obsclim_pr_30arcsec_global_daily_201012.nc"
#variable="pr"
variable="tasmin"
out_put_dir="./outputs/"
mkdir -p ${out_put_dir}
# namelist variables: 
# region: 
lat0=38.80
lat1=39.80
lon0=67.4
lon1=70.8
resolution="30arcsec"
# select domain :
for year in {1980..2016}
do 
  for month in {01..12}
  do 
    echo "processing the "$year "and "$month
    ncks -O -d lat,${lat0},${lat1} -d lon,${lon0},${lon1} ${dir_to_CHELSA}chelsa-w5e5v1.0_obsclim_${variable}_${resolution}_global_daily_${year}${month}.nc ${out_put_dir}chelsa_CA_${year}${month}.nc 

  done # month 
done # year
# now merger everything? 

###cdo -L -mergetime  ${out_put_dir}chelsa_CA_*.nc  ${out_put_dir}chelsa_CA_1980_1989.nc 


