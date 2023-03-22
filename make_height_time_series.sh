#!/bin/bash
set -e x
################################ CHELSA #######################################
#resolution="30arcsec"
resolution="90arcsec"
output_dir="./output/"
mkdir -p ${output_dir}

for year in {1980..2014}
do
    # make the mask for location of zero degree grid points
    # plus minus 0.5
    if [ "${resolution}" == "90arcsec"  ]
    then
	cdo -L -O -setvrange,272.6,273.65 ../outputs/chelsa_CA_${year}.nc temp.nc
	cdo -b f32 div temp.nc temp.nc mask.nc
	cdo -L -b f32 -fldmean -mul mask.nc ../chelsa-w5e5v1.0_obsclim_orog_${resolution}_Zarafshan.nc ${output_dir}height_zdi_${year}_${resolution}.nc
	rm temp.nc mask.nc
    
    else
	cdo -L -O -setvrange,272.6,273.65 ../outputs/chelsa_CA_${year}_${resolution}.nc temp.nc
	cdo -b f32 div temp.nc temp.nc mask.nc
	# multiply by the topography dataset:
	cdo -L -b f32 -fldmean -mul mask.nc ../chelsa-w5e5v1.0_obsclim_orog_${resolution}_Zarafshan.nc ${output_dir}height_zdi_${year}_${resolution}.nc
	rm temp.nc mask.nc
    
    fi 

done

cdo -L mergetime ${output_dir}height_zdi_*_${resolution}.nc ${output_dir}height_zdi_1980_2016_${resolution}.nc
